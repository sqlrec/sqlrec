"""Select a model, train on the full data, predict, report and release services."""
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import argparse
import logging
import os
import signal
import time
import uuid

from .client import APIClient, HS2Client
from .common import stable_id, write_json
from .config import DATASETS, MODELS, PACKAGE, applicable_models, dataset_definition, environment, view_fields
from .data import load_dataset, training_table
from .metrics import ranking_metrics, retrieval_metrics
from .ranking import run_ranking
from .report import write_report
from .resources import RunResources
from .retrieval import run_retrieval


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model', choices=(*MODELS, 'all'), required=True)
    parser.add_argument('--dataset', choices=(*DATASETS, 'all'), default='all')
    parser.add_argument('--full-eval', action='store_true', help='Use all valid rows/queries instead of a fixed sample')
    parser.add_argument('--save-predictions', action='store_true', help='Save one compact predictions.parquet per model')
    parser.add_argument('--data-root', type=Path, default=Path(os.environ.get('BENCH_DATA_ROOT', PACKAGE / 'data')))
    parser.add_argument('--output', type=Path, default=PACKAGE / 'runs', help='Parent directory for a new isolated run')
    args = parser.parse_args()
    if args.model == 'mmoe' and args.dataset not in ('all', 'kuairand'):
        parser.error('MMoE is available only on KuaiRand')
    return args


def create_run_directory(args, datasets):
    dataset = datasets[0] if len(datasets) == 1 else 'all-datasets'
    model = 'all-models' if args.model == 'all' else args.model
    name = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + f'_{dataset}_{model}'
    if args.full_eval:
        name += '_full'
    if args.save_predictions:
        name += '_predictions'
    number = 1
    while True:
        root = args.output.resolve() / (name if number == 1 else f'{name}_{number}')
        try:
            root.mkdir(parents=True)
            return root
        except FileExistsError:
            number += 1


def main():
    args = arguments()
    names = DATASETS if args.dataset == 'all' else [args.dataset]
    if args.model == 'mmoe':
        names = ['kuairand']
    root = create_run_directory(args, names)
    run_id = uuid.uuid4().hex  # Internal resource identity, independent of the readable folder name.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(root / 'run.log'), logging.StreamHandler()])
    results = []
    for name in names:
        definition = dataset_definition(name)
        for model in applicable_models(name, args.model):
            _, tasks = view_fields(definition, model)
            results.append({'dataset': name, 'model': model, 'tasks': [task.id for task in tasks],
                            'kind': 'retrieval' if model == 'dssm' else 'ranking',
                            'candidate_scope': definition['protocol']['candidate_scope'], 'status': 'NOT_EXECUTED'})
    write_report(root, results, args.full_eval)
    write_json(root / 'config.json', {'model': args.model, 'datasets': list(names), 'full_eval': args.full_eval,
                                      'save_predictions': args.save_predictions, 'data_root': str(args.data_root.resolve())})
    env, warehouse = None, None
    active = results[0]
    exit_code = 0
    try:
        env = environment()
        write_json(root / 'environment.json', asdict(env))
        warehouse = HS2Client(env.endpoints['warehouse']).connect()
        for name in names:
            active = next(result for result in results if result['dataset'] == name)
            logging.info('Loading and checking %s', name)
            start = time.monotonic()
            dataset = load_dataset(name, args.data_root, args.full_eval)
            load_seconds = time.monotonic() - start
            for result in [item for item in results if item['dataset'] == name]:
                active = result
                model = result['model']
                start = time.monotonic()
                result.update(status='FAILED', data_version=dataset.manifest['fingerprint'],
                              preparation_version=dataset.manifest['preparation_version'], train_rows=len(dataset.train))
                client, api, resources = None, None, None
                error = None
                try:
                    client = HS2Client(env.endpoints['sqlrec']).connect()
                    client.execute('SET table.sql-dialect = default')
                    api = APIClient(env.endpoints['api'])
                    prefix = 'mq_' + stable_id(run_id, name, model)[:20]
                    resources = RunResources(prefix, env, root / name / model, client)
                    with resources.stage('training_input'):
                        table = training_table(dataset, model, warehouse, env)
                    if model == 'dssm':
                        queries, predictions = run_retrieval(dataset, table, resources, api, args.save_predictions)
                        result['metrics'] = retrieval_metrics(queries, predictions, set(dataset.items.video_id))
                        del queries, predictions
                    else:
                        observations, predictions, tasks, outputs = run_ranking(dataset, model, table, resources, api, args.save_predictions)
                        result['metrics'] = ranking_metrics(observations, predictions, tasks, outputs)
                        del observations, predictions
                except BaseException as failure:
                    error = failure
                finally:
                    # Allow one interrupt to start cleanup; a second must not abandon it.
                    previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
                    try:
                        if resources:
                            try:
                                resources.cleanup()
                            except BaseException as failure:
                                result['cleanup_error'] = str(failure)
                                resources.status = 'cleanup_failed'
                                resources.save()
                                error = error or failure
                            result['stage_seconds'] = {'load': load_seconds, **resources.times}
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
                    if result.get('cleanup_error'):
                        result['error'] += '; cleanup: ' + result['cleanup_error']
                    raise error
                result['status'] = 'COMPLETED'
                write_report(root, results, args.full_eval)
                logging.info('%s/%s completed in %.1fs', name, model, result['seconds'])
    except BaseException as failure:
        active['status'] = 'FAILED'
        active.setdefault('error', f'{type(failure).__name__}: {failure}')
        logging.error('%s', active['error'])
        exit_code = 130 if isinstance(failure, KeyboardInterrupt) else 1
    finally:
        if warehouse:
            warehouse.close()
        write_report(root, results, args.full_eval)
        logging.info('Report: %s', root / 'summary.md')
    return exit_code


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
