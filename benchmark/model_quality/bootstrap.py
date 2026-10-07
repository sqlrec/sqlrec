"""Standard-library bootstrap and failure reports, usable before pip dependencies."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import importlib
import importlib.metadata
import importlib.util
import json
import os
import signal
from pathlib import Path
import subprocess
import sys


def read(path):
    return json.loads(Path(path).read_text()) if Path(path).is_file() else {}


def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.partial')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + '\n')
    temporary.replace(path)


def stage(root, name, status, message=None):
    path = Path(root) / 'setup.json'; value = read(path)
    entry = value.setdefault('stages', {}).setdefault(name, {})
    entry.update(status=status, updated_at=datetime.now(timezone.utc).isoformat())
    if message:
        entry['message'] = message
    write(path, value)


def dependency_command(command):
    """Terminate pip/build children when the bootstrap process is interrupted."""
    process = subprocess.Popen(command, start_new_session=True)
    try:
        status = process.wait()
        if status:
            raise subprocess.CalledProcessError(status, command)
    except BaseException:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
        raise


def ensure_dependencies(requirements, root):
    if importlib.util.find_spec('pip') is None:
        dependency_command([sys.executable, '-m', 'ensurepip'])
    # pip bundles a PEP 440 parser; bootstrap itself needs no installed libraries.
    from pip._vendor.packaging.requirements import Requirement
    requirements = Path(requirements)
    entries = [Requirement(line.split('#', 1)[0].strip()) for line in requirements.read_text().splitlines()
               if line.split('#', 1)[0].strip()]
    def versions():
        result, missing = {}, []
        for entry in entries:
            if entry.marker and not entry.marker.evaluate():
                continue
            try:
                version = importlib.metadata.version(entry.name)
                result[entry.name] = version
                if not entry.specifier.contains(version):
                    missing.append(str(entry))
            except importlib.metadata.PackageNotFoundError:
                missing.append(str(entry))
        return result, missing
    installed, missing = versions()
    checked = subprocess.run([sys.executable, '-m', 'pip', 'check'], capture_output=True, text=True)
    changed = bool(missing) or checked.returncode != 0
    if changed:
        print('Installing benchmark dependencies.', flush=True)
        dependency_command([sys.executable, '-m', 'pip', 'install', '-r', str(requirements)])
        installed, missing = versions()
    if missing:
        raise ValueError('Unsatisfied benchmark requirements: ' + ', '.join(missing))
    subprocess.run([sys.executable, '-m', 'pip', 'check'], check=True)
    for module in ('numpy', 'pandas', 'pyarrow', 'requests', 'yaml', 'sklearn', 'pyhive', 'TCLIService', 'thrift_sasl', 'puresasl'):
        importlib.import_module(module)
    path = Path(root) / 'setup.json'; value = read(path)
    value['environment'] = {'python': sys.executable, 'python_version': sys.version,
                            'requirements_sha256': hashlib.sha256(requirements.read_bytes()).hexdigest(),
                            'packages': installed, 'dependencies_installed': changed}
    write(path, value)
    print('Benchmark dependencies ready; ' + ('installed/updated.' if changed else 'reused existing environment.'), flush=True)


def finalize(root, exit_code):
    root = Path(root); setup = read(root / 'setup.json')
    for entry in setup.get('stages', {}).values():
        if entry['status'] == 'running':
            entry['status'] = 'interrupted' if exit_code else 'failed'
    setup['exit_code'] = exit_code; write(root / 'setup.json', setup)
    summary = read(root / 'summary.json')
    if not summary:
        runs = []
        for path in setup.get('workflow_paths', []):
            directory = Path(path); journal = read(directory / 'execution/journal.json')
            attempted = directory.exists()
            status = 'cleanup_failed' if journal.get('resource_status') == 'blocked' else 'workflow_failed' if attempted else 'not_executed'
            runs.append({'run': directory.name, 'path': path, 'status': status,
                         'suite_status': 'failed' if attempted else 'not_executed', 'formal': False,
                         'reason': 'suite_failed_before_summary', 'model_states': journal.get('models', {})})
        summary = {'runs': runs, 'metrics': [], 'quality_status': 'not_assessed'}

    counts = {name: sum(r.get('suite_status') == name for r in summary['runs'])
              for name in ('completed', 'failed', 'not_executed')}
    summary.update(status='completed' if exit_code == 0 else 'failed', exit_code=exit_code,
                   setup=setup, completed_runs=counts['completed'], failed_runs=counts['failed'],
                   not_executed_runs=counts['not_executed'])
    write(root / 'summary.json', summary)
    if not (root / 'metrics.csv').exists():
        with (root / 'metrics.csv').open('w') as stream:
            csv.writer(stream).writerow(['run', 'dataset', 'profile', 'feature_view', 'candidate_scope', 'formal', 'status',
                                        'suite_status', 'model', 'seed', 'slice', 'task', 'metric', 'value'])
    path = root / 'summary.md'
    previous = path.read_text() if path.is_file() else '# Model quality suite\n\nNo model quality result was produced.\n'
    rows = ['\n## Setup and execution\n',
            f'Suite exit code: {exit_code}. Completed configurations: {counts["completed"]}; '
            f'failed: {counts["failed"]}; not executed: {counts["not_executed"]}.\n',
            '| Stage | Status | Log |', '| --- | --- | --- |']
    rows += [f'| {name} | {entry["status"]} | {entry.get("message", "")} |'
             for name, entry in setup.get('stages', {}).items()]
    path.write_text(previous + '\n'.join(rows) + '\n')
    print(f'Suite report: {path}', flush=True)
    print(f'Completed={counts["completed"]}; failed={counts["failed"]}; not executed={counts["not_executed"]}.', flush=True)


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('init', 'dependencies', 'status', 'finalize'))
    parser.add_argument('--run', required=True)
    parser.add_argument('--inputs', nargs='*', default=[])
    parser.add_argument('--requirements')
    parser.add_argument('--stage')
    parser.add_argument('--status', choices=('running', 'succeeded', 'failed'))
    parser.add_argument('--message')
    parser.add_argument('--exit-code', type=int, default=0)
    args = parser.parse_args()
    if args.action == 'init':
        write(Path(args.run) / 'setup.json', {'workflow_paths': args.inputs, 'stages': {}})
    elif args.action == 'dependencies':
        if not args.requirements:
            parser.error('dependencies requires --requirements')
        ensure_dependencies(args.requirements, args.run)
    elif args.action == 'status':
        if not args.stage or not args.status:
            parser.error('status requires --stage and --status')
        stage(args.run, args.stage, args.status, args.message)
    else:
        finalize(args.run, args.exit_code)


if __name__ == '__main__':
    main()
