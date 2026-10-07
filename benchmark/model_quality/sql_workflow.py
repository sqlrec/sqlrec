"""Build, execute and evaluate SQLRec's public SQL/API model workflow."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import signal

import yaml

from .common import read_json, write_json
from .user_workflow.infrastructure import IndexAdmin
from .user_workflow.configuration import resolve_config
from .user_workflow.recipes import build
from .user_workflow.results import evaluate, summarize_runs
from .user_workflow.runner import Runner, verify_plan
from .user_workflow.runtime_audit import RuntimeAudit


def upload_data(root):
    """Upload frozen training directories with the normal Hadoop client."""
    root = Path(root).resolve()
    plan = verify_plan(root)
    audit = plan['config']['execution'].get('runtime_audit', {})
    command = ([os.environ['HADOOP_BIN']] if os.environ.get('HADOOP_BIN') else
               audit.get('hadoop') or ['hadoop'])
    if not shutil.which(command[0]):
        raise ValueError('Set HADOOP_BIN or execution.runtime_audit.hadoop for uploads')
    tables = ['samples']
    if any(model['family'] == 'tzrec.dssm' for model in plan['config']['models']):
        tables.append('positive')
    sources = [root / 'data' / table for table in tables]
    if any(not path.is_dir() for path in sources):
        raise ValueError('Frozen training directories are missing')
    env = dict(os.environ)
    if audit.get('java_home') and not env.get('JAVA_HOME'):
        env['JAVA_HOME'] = audit['java_home']
    uri = plan['config']['dataset']['storage_uri'].rstrip('/')
    subprocess.run(command + ['fs', '-mkdir', '-p', uri], env=env, check=True)
    # Keep Hadoop's collision checks: never overwrite an existing training directory.
    subprocess.run(command + ['fs', '-put', *map(str, sources), uri + '/'], env=env, check=True)
    return {'storage_uri': uri, 'uploaded_tables': tables}


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['build', 'upload-data', 'run', 'evaluate', 'provision-index', 'check', 'audit-runtime', 'cleanup-services', 'summarize', 'preflight', 'prepare-data'])
    parser.add_argument('--config', help='YAML/JSON, required for build')
    parser.add_argument('--run', required=True, help='New directory for build; immutable directory otherwise')
    parser.add_argument('--prepared-inputs', help='Build: suite prepared_inputs.json selecting verified dataset cache paths')
    parser.add_argument('--profile', choices=['smoke', 'full'], help='Build: sampling profile (compact configs default to smoke)')
    parser.add_argument('--run-id', help='Build: unique suffix for SQL objects and training storage')
    parser.add_argument('--inputs', nargs='+', help='Summarize: workflow directories; preflight/prepare-data: experiment YAML paths')
    parser.add_argument('--failed-run', action='append', default=[], help='Summarize: workflow failed, including report or cleanup failures')
    parser.add_argument('--not-executed', action='append', default=[], help='Summarize: skipped after a resource barrier failed')
    parser.add_argument('--through', help='Stop after this public stage (not a successful full run)')
    parser.add_argument('--retry-failed-create', action='append', default=[],
                        help='Operator retry of a failed CREATE TABLE only; never an uncertain write')
    parser.add_argument('--model', help='Resolved SQL model name for audit-runtime')
    parser.add_argument('--artifact', help='Unmodified normal training pipeline.config, read from public storage')
    parser.add_argument('--diagnostic', action='store_true', help='Report only complete metric inputs; never formal/pass')
    parser.add_argument('--require-formal', action='store_true',
                        help='For evaluate: write the report and exit 1 unless formal=true')
    args = parser.parse_args(); root = Path(args.run)
    if args.require_formal and args.action != 'evaluate':
        parser.error('--require-formal is only supported by evaluate')
    if (args.profile or args.run_id) and args.action not in ('build', 'preflight', 'prepare-data'):
        parser.error('--profile and --run-id only apply to build/preflight/prepare-data')
    if args.inputs and args.action not in ('summarize', 'preflight', 'prepare-data'):
        parser.error('--inputs only applies to summarize/preflight/prepare-data')
    if (args.failed_run or args.not_executed) and args.action != 'summarize':
        parser.error('--failed-run and --not-executed only apply to summarize')
    if args.prepared_inputs and args.action != 'build':
        parser.error('--prepared-inputs only applies to build')
    if args.action == 'build':
        if not args.config:
            parser.error('build requires --config')
        inputs = read_json(args.prepared_inputs) if args.prepared_inputs else None
        config = resolve_config(yaml.safe_load(Path(args.config).read_text()), args.config, args.profile, args.run_id, prepared_inputs=inputs)
        value = build(config, root)
        print(json.dumps({'plan_id': value['plan_id'], 'steps': len(value['steps']), 'run': str(root)}))
    elif args.action in ('preflight', 'prepare-data'):
        from .user_workflow.data_setup import preflight, prepare_suite
        if not args.inputs:
            parser.error('preflight/prepare-data requires --inputs with experiment YAML paths')
        action = preflight if args.action == 'preflight' else prepare_suite
        action(args.inputs, root, args.profile or 'smoke', args.run_id)
        print(json.dumps({'action': args.action, 'status': 'succeeded'}))
    elif args.action == 'summarize':
        if not args.inputs:
            parser.error('summarize requires --inputs')
        result = summarize_runs(root, args.inputs, args.failed_run, args.not_executed)
        print(json.dumps({'runs': len(result['runs']), 'missing_reports': result['missing_reports'], 'report': str(root/'summary.md')}))
    elif args.action == 'cleanup-services':
        result = Runner(root).cleanup_services()
        print(json.dumps({'released_services': sum(s['status'] == 'succeeded' for s in result['services'].values())}))
    elif args.action == 'upload-data':
        print(json.dumps(upload_data(root)))
    elif args.action == 'run':
        if args.through and args.through not in {s['id'] for s in verify_plan(root)['steps']}:
            parser.error('Unknown --through stage')
        result = Runner(root).run(args.through, args.retry_failed_create)
        print(json.dumps({'status': result['status'], 'completed': sum(s['status']=='succeeded' for s in result['steps'].values())}))
    elif args.action == 'evaluate':
        result = evaluate(root,args.diagnostic)
        name = 'diagnostic_report.md' if args.diagnostic else 'report.md'
        print(json.dumps({'status': result['status'], 'formal': result['formal'],
                          'quality_status': result['quality_status'], 'report': str(root/'results'/name)}))
        if args.require_formal and not result['formal']:
            raise SystemExit(1)
    elif args.action == 'provision-index':
        plan = verify_plan(root)
        indexes = [step for step in plan['steps'] if step['endpoint'] == 'infra'
                   and step['capture']['kind'] == 'index_empty']
        if indexes:
            admin = IndexAdmin(plan['config']['sql']['endpoints']['milvus'])
            for step in indexes:
                spec = read_json(root / step['capture']['spec'])
                admin.provision(spec)
                proof = admin.verify(spec, empty=True)
                write_json(root / f'infra/{spec["collection"]}_provisioned.json', proof)
        print(f'Provisioned {len(indexes)} isolated index schema(s); no vectors inserted.')
    elif args.action == 'audit-runtime':
        plan = verify_plan(root)
        if not args.model:
            parser.error('audit-runtime requires --model')
        evaluation = next((e for e in plan['evaluations'] if e['id']==args.model),None)
        if evaluation is None:
            parser.error('Unknown model in this plan')
        recipe = next(r for r in plan['config']['models'] if r['id']==evaluation['recipe_id'])
        result = RuntimeAudit(root,plan).checkpoint(args.model,recipe,evaluation['seed'],args.artifact)
        if result is None:
            parser.error('Set execution.runtime_audit or supply --artifact')
        print(json.dumps(result,ensure_ascii=False))
    else:
        plan = verify_plan(root); print(json.dumps({'plan_id': plan['plan_id'], 'files_verified': len(plan['files'])}))


if __name__ == '__main__':
    main()
