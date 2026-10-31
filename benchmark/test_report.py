"""Standard-library report collector for bin/test.sh -all; no cluster access."""
from datetime import datetime
from pathlib import Path
import json
import os
import re
import shutil
import sys
import time
import uuid
import xml.etree.ElementTree as ET


STAGES = {
    'cache-images': 'Cache existing Minikube images',
    'delete-minikube': 'Delete existing Minikube environment',
    'prepare-minikube': 'Prepare Minikube',
    'cluster-address': 'Discover cluster address',
    'download-resources': 'Download deployment resources',
    'deploy-components': 'Deploy SQLRec and dependencies',
    'deploy-mongodb': 'Deploy MongoDB',
    'deploy-redis': 'Deploy Redis Cluster',
    'deploy-growthbook': 'Deploy GrowthBook',
    'wait-workloads': 'Wait for deployed workloads',
    'sqlrec-ready': 'Verify SQLRec connection',
    'maven': 'Maven tests',
    'sql-prepare': 'Prepare SQL performance benchmark',
    'sql-performance': 'SQL performance benchmark',
    'python-dependencies': 'Install model benchmark dependencies',
    'growthbook': 'GrowthBook UDF benchmark',
    'huggingface': 'Hugging Face integration benchmark',
    'model-prepare': 'Prepare all model quality datasets',
    'model-quality': 'All model quality benchmarks (full evaluation)',
}
BENCHMARKS = {'growthbook': 'growthbook', 'huggingface': 'huggingface', 'model-quality': 'model_quality'}


def write_json(path, value):
    temporary = path.with_suffix('.json.partial')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    temporary.replace(path)


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def timestamp(seconds):
    return datetime.fromtimestamp(seconds).astimezone().isoformat(timespec='seconds')


def maven_files(project):
    """Only reactor modules, without traversing datasets or submodules."""
    pending, visited = [project], set()
    while pending:
        module = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        pom = module / 'pom.xml'
        if pom.is_file():
            tree = ET.parse(pom)
            pending.extend((module / child.text).resolve()
                           for child in tree.findall('.//{*}modules/{*}module') if child.text)
        for kind in ('surefire', 'failsafe'):
            yield from (module / 'target' / (kind + '-reports')).glob('TEST-*.xml')


def maven_snapshot(project):
    return {str(path): [path.stat().st_mtime_ns, path.stat().st_size] for path in maven_files(project)}


def collect_maven(root, state):
    project = Path(state['project'])
    before = state.get('maven_before', {})
    reports, errors = [], []
    for path in maven_files(project):
        if before.get(str(path)) == [path.stat().st_mtime_ns, path.stat().st_size]:
            continue
        relative = path.relative_to(project)
        saved = root / 'maven' / relative
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, saved)
        try:
            tree = ET.parse(saved)
            suites = [tree.getroot()] if tree.getroot().tag == 'testsuite' else tree.findall('.//testsuite')
            for suite in suites:
                failures = []
                for case in suite.findall('testcase'):
                    for kind in ('failure', 'error'):
                        for failure in case.findall(kind):
                            failures.append({'test': case.get('classname', '') + '.' + case.get('name', ''),
                                             'kind': kind, 'message': failure.get('message', ''),
                                             'detail': failure.text or ''})
                reports.append({'module': str(relative.parent.parent.parent), 'suite': suite.get('name'),
                                **{key: int(suite.get(key, 0)) for key in ('tests', 'failures', 'errors', 'skipped')},
                                'seconds': float(suite.get('time', 0)), 'failed_tests': failures,
                                'xml': saved.relative_to(root).as_posix()})
        except (ET.ParseError, ValueError) as failure:
            errors.append(f'{relative}: {failure}')
    state['maven'] = {'reports': reports, 'collection_errors': errors}


def sql_metrics(log):
    """Read only the formal wrk summary, never the warm-up measurements."""
    if 'Benchmark configuration:' not in log:
        return {}
    formal = log.split('Benchmark configuration:', 1)[1]
    patterns = {
        'concurrency': r'Concurrency=(\d+)', 'duration_seconds': r'Duration=(\d+)s',
        'requests': r'^\s*(\d+) requests in ', 'requests_per_second': r'^Requests/sec:\s+([\d.]+)',
        'latency_mean': r'^\s*Latency\s+(\S+)',
        'latency_p50': r'^\s*50%\s+(\S+)', 'latency_p75': r'^\s*75%\s+(\S+)',
        'latency_p90': r'^\s*90%\s+(\S+)', 'latency_p99': r'^\s*99%\s+(\S+)',
        'transfer_per_second': r'^Transfer/sec:\s+(\S+)',
        'http_errors': r'^\s*Non-2xx or 3xx responses:\s*(\d+)',
    }
    values = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, formal, re.MULTILINE)
        if match:
            value = match.group(1)
            values[key] = float(value) if key == 'requests_per_second' else int(value) if value.isdigit() else value
    # wrk omits these lines when the measured counts are zero. Only infer zero
    # after both summary markers have been emitted by a completed measurement.
    complete = 'requests' in values and 'requests_per_second' in values
    socket = re.search(r'^\s*Socket errors: connect (\d+), read (\d+), write (\d+), timeout (\d+)', formal, re.MULTILINE)
    for index, key in enumerate(('connect_errors', 'read_errors', 'write_errors', 'timeouts')):
        if socket:
            values[key] = int(socket.group(index + 1))
        elif complete:
            values[key] = 0
    if complete:
        values.setdefault('http_errors', 0)
    return values


def collect_benchmarks(root, state):
    results = {}
    state['collection_errors'] = []
    for stage, directory in BENCHMARKS.items():
        path = root / directory / 'results.json'
        if path.is_file():
            try:
                result = read_json(path)
                if not isinstance(result, dict):
                    raise ValueError('Expected a result object')
                if stage != 'growthbook' and not isinstance(result.get('results'), list):
                    raise ValueError('Expected a results array')
                results[stage] = result
            except ValueError as error:
                state.setdefault('collection_errors', []).append(f'{directory}/results.json: {error}')
    log = root / 'logs/sql-performance.log'
    if log.is_file():
        results['sql-performance'] = sql_metrics(log.read_text(encoding='utf-8', errors='replace'))
        directory = root / 'sql_performance'
        directory.mkdir(exist_ok=True)
        write_json(directory / 'results.json', results['sql-performance'])
    state['benchmarks'] = results


def cell(value):
    return str(value if value is not None else 'N/A').replace('|', '\\|').replace('\n', '<br>')


def number(value):
    return 'N/A' if value is None else f'{value:.4f}'


def table(lines, columns, rows):
    lines.extend(['| ' + ' | '.join(columns) + ' |', '| ' + ' | '.join('---' for _ in columns) + ' |'])
    lines.extend('| ' + ' | '.join(cell(value) for value in row) + ' |' for row in rows)
    lines.append('')


def details(lines, label, value):
    if value:
        # Indentation keeps arbitrary stack traces/Markdown inside a code block.
        lines.extend([f'**{label}**', '', *('    ' + line for line in str(value).splitlines()), ''])


def stage_note(stage):
    return stage.get('reason') or ('See stage log for failure details.' if stage['status'] == 'FAILED' else '')


def render(root, state):
    lines = ['# SQLRec full test report', '', f'Status: **{state["status"]}**', '',
             f'Started: {timestamp(state["started"])}', '',
             f'Finished: {timestamp(state["finished"]) if state.get("finished") else "N/A"}', '',
             f'Elapsed: {state.get("seconds", time.time() - state["started"]):.1f}s', '',
             f'Maven arguments: {cell(json.dumps(state["maven_args"], ensure_ascii=False))}', '',
             'Execution stops at the first failed stage. Later stages are listed with the reason they did not run.', '',
             '## Stages', '']
    table(lines, ['Stage', 'Status', 'Seconds', 'Exit code', 'Log / reason'],
          [(stage['name'], stage['status'], number(stage.get('seconds')), stage.get('exit_code'),
            (f'[log]({stage["log"]}) ' if stage.get('started') else '') + stage_note(stage))
           for stage in state['stages'].values()])
    for stage in state['stages'].values():
        if stage['status'] in ('FAILED', 'INTERRUPTED'):
            log = root / stage['log']
            if log.is_file():
                details(lines, stage['name'] + ' — last 40 log lines',
                        '\n'.join(log.read_text(encoding='utf-8', errors='replace').splitlines()[-40:]))
    maven = state.get('maven', {})
    reports = maven.get('reports', [])
    lines.extend(['## Maven tests', ''])
    if reports:
        totals = {key: sum(report[key] for report in reports) for key in ('tests', 'failures', 'errors', 'skipped')}
        lines.extend([f'Tests: {totals["tests"]}; failures: {totals["failures"]}; errors: {totals["errors"]}; skipped: {totals["skipped"]}.', ''])
        table(lines, ['Module / suite', 'Tests', 'Failures', 'Errors', 'Skipped', 'Seconds', 'XML'],
              [(f'{r["module"]} / {r["suite"]}', r['tests'], r['failures'], r['errors'], r['skipped'],
                number(r['seconds']), f'[XML]({r["xml"]})') for r in reports])
        for report in reports:
            for failure in report['failed_tests']:
                details(lines, failure['test'] + ' (' + failure['kind'] + ')',
                        failure['message'] + '\n' + failure['detail'])
    else:
        lines.extend(['N/A — no new Maven test XML was produced. Historical reports are excluded. '
                      'See the Maven stage status/log for build failures or skipped tests.', ''])
    for error in maven.get('collection_errors', []):
        details(lines, 'Maven report collection error', error)
    benchmarks = state.get('benchmarks', {})
    for stage, heading in [('sql-performance', 'SQL performance'), ('growthbook', 'GrowthBook'),
                           ('huggingface', 'Hugging Face'), ('model-quality', 'Model quality')]:
        lines.extend(['## ' + heading, ''])
        entry = state['stages'][stage]
        lines.extend([f'Stage: {entry["status"]}. {stage_note(entry)}', ''])
        result = benchmarks.get(stage)
        if result is None:
            lines.extend(['N/A — no results were produced for this run.', ''])
            continue
        directory = BENCHMARKS.get(stage, 'sql_performance')
        lines.extend([f'[Structured results]({directory}/results.json)' +
                      (f' · [Original report]({directory}/summary.md)' if (root / directory / 'summary.md').is_file() else ''), ''])
        if stage == 'sql-performance':
            table(lines, ['Metric', 'Value'], [(key, result.get(key)) for key in (
                'concurrency', 'duration_seconds', 'requests', 'requests_per_second', 'latency_mean',
                'latency_p50', 'latency_p75', 'latency_p90', 'latency_p99', 'http_errors',
                'connect_errors', 'read_errors', 'write_errors', 'timeouts', 'transfer_per_second')])
            lines.extend(['Only the formal measurement is included. Requests/sec is wrk throughput, including error responses; '
                          'HTTP errors count non-2xx/3xx responses. This report does not add a performance pass threshold.', ''])
        elif stage == 'growthbook':
            table(lines, ['Check', 'Result'], [('Status', result.get('status')),
                  *list(result.get('metrics', {}).items()), ('GrowthBook cleanup', result.get('growthbook_cleanup')),
                  ('SQL cleanup', result.get('sql_cleanup'))])
            for key in ('error', 'cleanup_error'):
                details(lines, key, result.get(key))
        elif stage == 'huggingface':
            table(lines, ['Case / task', 'Status', 'Download (s)', 'Deploy (s)', 'Median batch (ms)', 'Rows/s', 'Rejected inputs'],
                  [(f'{r["case"]} / {r["task"]}', r['status'], number(r.get('stage_seconds', {}).get('download')),
                    number(r.get('stage_seconds', {}).get('deploy')), number(r.get('metrics', {}).get('median_batch_ms')),
                    number(r.get('metrics', {}).get('rows_per_second')), r.get('metrics', {}).get('invalid_requests_rejected'))
                   for r in result['results']])
            lines.extend(['Tiny random models test integration contracts; these are not model quality measurements.', ''])
        else:
            lines.extend([f'Evaluation: {result.get("evaluation", "N/A")}. COMPLETED describes execution, not a quality threshold.', ''])
            for kind in ('ranking', 'retrieval'):
                rows = []
                for r in result['results']:
                    if r['kind'] != kind:
                        continue
                    metrics = r.get('metrics', {})
                    tasks = {'retrieval': metrics} if kind == 'retrieval' else metrics or {task: {} for task in r['tasks']}
                    for task, values in tasks.items():
                        primary, secondary, count = ('auc', 'logloss', 'rows') if kind == 'ranking' else ('recall_at_50', 'ndcg_at_50', 'queries')
                        rows.append((r['dataset'], r['model'], r.get('candidate_scope') if kind == 'retrieval' else task,
                                     number(values.get(primary)), number(values.get(secondary)), values.get(count),
                                     number(r.get('seconds')), r['status']))
                if rows:
                    table(lines, ['Dataset', 'Model', 'Task / candidates',
                                  'AUC ↑' if kind == 'ranking' else 'Recall@50 ↑',
                                  'LogLoss ↓' if kind == 'ranking' else 'NDCG@50 ↑', 'Rows / queries', 'Seconds', 'Status'], rows)
        if stage in ('huggingface', 'model-quality'):
            for item in result['results']:
                label = item.get('case') or item['dataset'] + '/' + item['model']
                for key in ('error', 'cleanup_error'):
                    details(lines, label + ' ' + key, item.get(key))
                if stage == 'model-quality' and item['kind'] == 'ranking':
                    for task, values in item.get('metrics', {}).items():
                        details(lines, label + '/' + task + ' AUC N/A', values.get('auc_reason'))
            timings = [(item.get('case') or item['dataset'] + '/' + item['model'], key, number(value))
                       for item in result['results'] for key, value in item.get('stage_seconds', {}).items()]
            if timings:
                lines.extend(['### Stage timings', ''])
                table(lines, ['Case / model', 'Stage (including cleanup)', 'Seconds'], timings)
            if stage == 'model-quality':
                coverage = []
                for item in result['results']:
                    metrics = item.get('metrics', {})
                    tasks = {'retrieval': metrics} if item['kind'] == 'retrieval' else metrics
                    coverage.extend((item['dataset'] + '/' + item['model'], task, key, value)
                                    for task, values in tasks.items() for key, value in values.items()
                                    if key not in ('auc', 'logloss', 'recall_at_50', 'ndcg_at_50', 'auc_reason'))
                if coverage:
                    lines.extend(['### Evaluation coverage', ''])
                    table(lines, ['Model', 'Task', 'Metric', 'Value'], coverage)
    for error in state.get('collection_errors', []):
        details(lines, 'Report collection error', error)
    temporary = root / 'summary.md.partial'
    temporary.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    temporary.replace(root / 'summary.md')


def initialize(project, maven_args):
    project = project.resolve()
    parent = Path(os.environ.get('TEST_REPORT_ROOT', project / 'benchmark/runs')).resolve()
    root = parent / (datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + uuid.uuid4().hex[:8])
    (root / 'logs').mkdir(parents=True)
    state = {'project': str(project), 'maven_args': maven_args, 'started': time.time(), 'status': 'RUNNING',
             'stages': {key: {'name': label, 'status': 'NOT_EXECUTED', 'log': f'logs/{key}.log'}
                        for key, label in STAGES.items()}}
    write_json(root / 'results.json', state)
    render(root, state)
    return root


def update(root, action, arguments):
    state = read_json(root / 'results.json')
    now = time.time()
    if action in ('start', 'finish'):
        key = arguments[0]
        stage = state['stages'][key]
        if action == 'start':
            stage.update(status='RUNNING', started=now)
            if key == 'maven':
                state['maven_before'] = maven_snapshot(Path(state['project']))
        else:
            code = int(arguments[1])
            status = 'INTERRUPTED' if code in (130, 143) else 'FAILED' if code else 'PASSED'
            stage.update(status=status, exit_code=code,
                         seconds=now - stage['started'])
            if key == 'maven':
                collect_maven(root, state)
    else:
        code = int(arguments[0])
        interrupted = code in (130, 143)
        for key, stage in state['stages'].items():
            if stage['status'] == 'RUNNING':
                stage.update(status='INTERRUPTED' if interrupted else 'FAILED', exit_code=code or 1,
                             seconds=now - stage['started'])
                if key == 'maven':
                    collect_maven(root, state)
        failed = [stage for stage in state['stages'].values() if stage['status'] in ('FAILED', 'INTERRUPTED')]
        reason = 'Run stopped before this stage.'
        if interrupted:
            reason = 'Interrupted by user/signal.'
        elif failed:
            reason = 'Stopped after ' + failed[0]['name'] + '.'
        for stage in state['stages'].values():
            if stage['status'] == 'NOT_EXECUTED':
                stage['reason'] = reason
        state.update(status='INTERRUPTED' if interrupted else 'FAILED' if code or failed else 'PASSED',
                     exit_code=code, finished=now, seconds=now - state['started'])
    collect_benchmarks(root, state)
    if action == 'finalize' and state['status'] == 'PASSED':
        # A zero command exit can still hide ignored Maven test failures, a
        # failed case, or a missing/corrupt report. Do not label that run PASSED.
        maven = state.get('maven', {})
        invalid = bool(state['collection_errors'] or maven.get('collection_errors'))
        invalid |= any(r['failures'] or r['errors'] for r in maven.get('reports', []))
        for stage in BENCHMARKS:
            result = state['benchmarks'].get(stage)
            if state['stages'][stage]['status'] == 'PASSED':
                invalid |= result is None
            if result:
                cases = [result] if stage == 'growthbook' else result['results']
                invalid |= any(r.get('status') != 'COMPLETED' or r.get('cleanup_error') for r in cases)
        if invalid:
            state.update(status='FAILED', exit_code=1)
    write_json(root / 'results.json', state)
    render(root, state)
    return 1 if state['status'] == 'FAILED' else 0


def main():
    action, directory, *arguments = sys.argv[1:]
    if action == 'init':
        print(initialize(Path(directory).resolve(), arguments))
        return 0
    if action not in ('start', 'finish', 'finalize'):
        raise ValueError('Expected init, start, finish or finalize')
    root = Path(directory)
    code = update(root, action, arguments)
    if action == 'finalize':
        print('[test] Overall status: ' + read_json(root / 'results.json')['status'])
    return code


if __name__ == '__main__':
    raise SystemExit(main())
