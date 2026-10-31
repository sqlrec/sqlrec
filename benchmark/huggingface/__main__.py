"""Download, deploy and exercise tiny Hugging Face models through public SQL APIs."""
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
import argparse
import logging
import os
import signal
import statistics
import time
import uuid

from benchmark.model_quality.client import APIClient, HS2Client, Indeterminate, SQLFailed
from benchmark.model_quality.common import literal, write_json
from benchmark.model_quality.config import Feature, environment
from benchmark.model_quality.data import fields, options
from benchmark.model_quality.resources import RunResources, kubernetes_name

from .checks import check_equivalent, check_predictions
from .config import ALLOW_PATTERNS, CASES, TASKS, input_rows


class HuggingFaceResources(RunResources):
    def download(self):
        # HF TRAIN downloads an origin checkpoint; it must not have an ON table
        # or invoke the train/export path used by recommendation models.
        job = {'name': kubernetes_name(self.name + '-v1') + '-job', 'kind': 'download',
               'checkpoints': ['v1'], 'status': 'submitted'}
        if (self.workloads.get('job', job['name'])
                or (self.workloads.get('pods', selector='job-name=' + job['name']) or {}).get('items')):
            raise ValueError('Download workload name already exists')
        self.jobs.append(job)
        self.save()
        with self.stage('download'):
            self.execute(f'TRAIN MODEL `{self.name}`\nCHECKPOINT = \'v1\'\n'
                         f'WITH ({options(self.env.train_resources)})', long=True,
                         on_submitted=lambda operation: self.capture_job(job))
            info = self.checkpoint('v1')
            if info.get('Status:') != 'succeeded' or info.get('Checkpoint Type:') != 'origin':
                raise Indeterminate('Download has no public succeeded origin checkpoint proof')
            job['status'] = 'completed'
            self.save()


def register_api(resources, case, service):
    function = resources.name + '_predict'
    columns = [Feature('row_id', 'STRING'), *case.fields]
    projection = ', '.join(f'`{column}`' for column in ('row_id', *case.output_columns))
    for sql in (f'CREATE SQL FUNCTION `{function}`',
                f'DEFINE INPUT TABLE observations ({fields(columns)})',
                f'CACHE TABLE scores AS CALL call_service({literal(service)}, observations)',
                f'CACHE TABLE predictions AS SELECT {projection} FROM scores',
                'RETURN predictions',
                f'CREATE API `{function}` WITH `{function}`'):
        resources.execute(sql)
    return function


def exercise(case, resources, api, function, repeats):
    inputs = input_rows(case)

    def predict(rows):
        # Readiness retries are excluded from measurements and assertions.
        return check_predictions(case, rows, api.call(
            function, {'data': {'observations': rows}}, retry=False))

    with resources.stage('checks'):
        reference = predict(inputs)  # Warm up before timing.
        singles = {}
        for row in inputs:
            singles.update(predict([row]))
        check_equivalent(case, reference, singles)
        check_equivalent(case, reference, predict(list(reversed(inputs))))
        # Identical inputs on opposite sides of an internal batch boundary.
        check_equivalent(case, {'same': {**reference['row_0'], 'row_id': 'same'}},
                         {'same': {**reference['row_2'], 'row_id': 'same'}})

        invalid = []
        for field in case.fields:
            invalid.append({**inputs[0], field.name: None})
            invalid.append({key: value for key, value in inputs[0].items() if key != field.name})
        if case.task == 'image-embedding':
            invalid.extend({**inputs[0], 'image_url': url} for url in (
                'file:///tmp/sqlrec-hf-benchmark.png', 'ftp://example.com/sqlrec-hf-benchmark.png'))
        for row in invalid:
            try:
                api.call(function, {'data': {'observations': [row]}}, retry=False)
            except SQLFailed:
                continue
            raise ValueError('Invalid model input was accepted')
        # Rejected requests must not leave serving in a failed state.
        check_equivalent(case, reference, predict(inputs))

    samples = []
    with resources.stage('predict'):
        for _ in range(repeats):
            start = time.monotonic()
            predictions = predict(inputs)
            samples.append(time.monotonic() - start)
            check_equivalent(case, reference, predictions)
    write_json(resources.root / 'predictions.json', list(reference.values()))
    return {'rows_per_request': len(inputs), 'requests': repeats, 'batch_seconds': samples,
            'median_batch_ms': statistics.median(samples) * 1000,
            'rows_per_second': len(inputs) * repeats / sum(samples), 'invalid_requests_rejected': len(invalid)}


def write_report(root, results):
    write_json(root / 'results.json', {'scope': 'integration', 'results': results})
    lines = ['# Hugging Face integration benchmark', '',
             'Tiny random models; output contracts and lifecycle checks, without quality thresholds.', '',
             '| Case | Task | Status | Download (s) | Deploy (s) | Median batch (ms) | Rows/s |',
             '| --- | --- | --- | --- | --- | --- | --- |']
    for result in results:
        timings = result.get('stage_seconds', {})
        metrics = result.get('metrics', {})
        lines.append(f'| {result["case"]} | {result["task"]} | {result["status"]} | '
                     f'{timings.get("download", 0):.2f} | {timings.get("deploy", 0):.2f} | '
                     f'{metrics.get("median_batch_ms", 0):.2f} | {metrics.get("rows_per_second", 0):.2f} |')
    lines.extend(['', 'Latency includes public SQL API transport and, for images, HTTP image downloads.', ''])
    for result in results:
        for key in ('error', 'cleanup_error'):
            if result.get(key):
                lines.append(f'- {result["case"]} {key}: {result[key]}')
    (root / 'summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')


def run_case(case, env, root, result, repeats):
    client = api = resources = None
    error = None
    result['status'] = 'FAILED'
    start = time.monotonic()
    try:
        client = HS2Client(env.endpoints['sqlrec']).connect()
        client.execute('SET table.sql-dialect = default')
        api = APIClient(env.endpoints['api'])
        name = 'hf_' + uuid.uuid4().hex[:20]
        resources = HuggingFaceResources(name, env, root / case.name, client)
        params = dict(model='huggingface.transformers', task=case.task, repo_id=case.repo_id,
                      revision=case.revision, version=os.environ.get('BENCH_TRANSFORMERS_VERSION',
                                                                   os.environ.get('SQLREC_VERSION', '0.1.18')),
                      allow_patterns=ALLOW_PATTERNS, trust_remote_code='false',
                      max_length=64, **case.parameters)
        write_json(resources.root / 'config.json', {'case': asdict(case), 'model_parameters': params,
                                                   'inputs': input_rows(case)})
        resources.define(case.fields, [], params, {'columns': {}})
        resources.download()
        service = resources.deploy(['v1'])['scalar']
        function = register_api(resources, case, service)
        result['metrics'] = exercise(case, resources, api, function, repeats)
    except BaseException as failure:
        error = failure
    finally:
        previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            if resources:
                try:
                    resources.cleanup()
                except BaseException as failure:
                    result['cleanup_error'] = f'{type(failure).__name__}: {failure}'
                    error = error or failure
                result['stage_seconds'] = dict(resources.times)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            if api:
                api.close()
            if resources:
                resources.client.close()
            elif client:
                client.close()
        result['seconds'] = time.monotonic() - start
    if error:
        result['error'] = f'{type(error).__name__}: {error}'
        raise error
    result['status'] = 'COMPLETED'


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', choices=(*TASKS, 'all'), default='all')
    parser.add_argument('--repeats', type=positive_integer, default=3, help='Timed three-row requests per case')
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'runs')
    parser.add_argument('--run-dir', type=Path, help='Exact new run directory (used by bin/test.sh -all)')
    args = parser.parse_args()
    cases = [case for case in CASES if args.task in ('all', case.task)]
    root = (args.run_dir.resolve() if args.run_dir else args.output.resolve() /
            (datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + uuid.uuid4().hex[:8]))
    root.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(root / 'run.log'), logging.StreamHandler()])
    results = [{'case': case.name, 'task': case.task, 'repo_id': case.repo_id,
                'revision': case.revision, 'status': 'NOT_EXECUTED'} for case in cases]
    active = results[0]
    write_report(root, results)
    exit_code = 0
    try:
        env = environment()
        # Small checkpoints run sequentially on CPU, with one service at a time.
        env = replace(env,
                      train_resources={**env.train_resources,
                                       'pod_cpu_cores': int(os.environ.get('BENCH_TRAIN_CPU', 1)),
                                       'pod_memory': os.environ.get('BENCH_TRAIN_MEMORY', '2Gi')},
                      service_resources={**env.service_resources, 'device': 'cpu', 'dtype': 'float32',
                                         'inference_batch_size': 2, 'replicas': 1})
        if env.train_resources['pod_cpu_cores'] < 1:
            raise ValueError('CPU resources must be positive')
        write_json(root / 'environment.json', asdict(env))
        for case, result in zip(cases, results):
            active = result
            logging.info('Running %s (%s)', case.name, case.repo_id)
            run_case(case, env, root, result, args.repeats)
            write_report(root, results)
    except BaseException as failure:
        active.update(status='FAILED', error=f'{type(failure).__name__}: {failure}')
        logging.error('%s', active['error'])
        exit_code = 130 if isinstance(failure, KeyboardInterrupt) else 1
    finally:
        write_report(root, results)
        logging.info('Report: %s', root / 'summary.md')
    return exit_code


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
