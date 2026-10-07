"""Identity/coverage validation and metrics on actual public returns only."""
import csv
from pathlib import Path

from ..common import captured_result, digest, read_json, write_json
from ..metrics import scalar_metrics, recall_metrics
from .runner import verify_plan


def metric_rows(result):
    """One projection for per-run CSV and suite reports."""
    if 'scalar' not in result:
        return
    scalar = {'overall': result['scalar'], **result.get('scalar_slices', {})}
    retrieval = {'overall': result.get('retrieval', {}), **result.get('retrieval_slices', {})}
    for label in sorted(set(scalar) | set(retrieval)):
        for task, metrics in {**scalar.get(label, {}), 'retrieval': retrieval.get(label, {})}.items():
            for metric, value in metrics.items():
                if isinstance(value, (int, float)) or value is None:
                    yield {'experiment': result['id'], 'recipe': result.get('recipe', result['id']), 'model': result['model'],
                           'seed': result['seed'], 'slice': label, 'task': task, 'metric': metric, 'value': value}


def evaluate(root, diagnostic=False):
    root = Path(root); plan = verify_plan(root); journal = read_json(root / 'execution/journal.json')
    complete = all(journal['steps'].get(s['id'], {}).get('status') == 'succeeded' for s in plan['steps'])
    if journal['plan_id'] != plan['plan_id'] or (not complete and not diagnostic):
        raise ValueError('Required public workflow stages have not all succeeded')
    for step in plan['steps']:
        record = journal['steps'].get(step['id'], {})
        if record.get('status') != 'succeeded' and not diagnostic:
            raise ValueError('Incomplete journal')
        if record.get('artifact') and digest(root / record['artifact']) != record['sha256']:
            raise ValueError('Public result/evidence checksum mismatch')
    bundle = read_json(root / 'data_contract.json'); results = []; long_rows = []
    steps = {s['id']: s for s in plan['steps']}
    def returned(name):
        record = journal['steps'][name]
        evidence = captured_result(steps[name]['capture'], read_json(root / record['artifact'])['results'])
        expected_source = 'public_sql_api' if plan['config']['serving']['entrypoint'] == 'sql_api' else 'public_sql'
        if evidence['source'] != expected_source:
            raise ValueError('Prediction source is not the declared public entrypoint')
        return evidence['rows']
    for evaluation in plan['evaluations']:
        if not all(journal['steps'].get(s,{}).get('status')=='succeeded' for s in evaluation['scalar_steps']):
            if diagnostic:
                results.append({'id':evaluation['id'], 'status':'incomplete_scalar_predictions'})
                continue
            raise ValueError('Incomplete scalar prediction coverage')
        predictions = [row for name in evaluation['scalar_steps'] for row in returned(name)]
        scalar = scalar_metrics(bundle['observations'], predictions, plan['tasks'], evaluation['outputs'],
                                evaluation['probability'], plan['config']['evaluation'].get('gauc_group'))
        recipe = evaluation['recipe_id']
        value = {'id': evaluation['id'], 'recipe':recipe, 'model': evaluation['model'], 'seed': evaluation['seed'], 'scalar': scalar}
        slices = {}
        for column in plan['config']['evaluation'].get('slice_by', []):
            values = sorted({str(row[column]) for row in bundle['observations']})
            for label in values:
                observations = [row for row in bundle['observations'] if str(row[column]) == label]
                ids = {row['row_id'] for row in observations}
                slices[f'{column}={label}'] = scalar_metrics(observations,
                    [row for row in predictions if row['row_id'] in ids], plan['tasks'], evaluation['outputs'],
                    evaluation['probability'], plan['config']['evaluation'].get('gauc_group'))
        value['scalar_slices'] = slices
        write_json(root / f'results/{evaluation["id"]}_scalar.json', predictions)
        recall_ready = all(journal['steps'].get(s,{}).get('status')=='succeeded' for s in evaluation.get('recall_steps',[]))
        if evaluation.get('recall_steps') and recall_ready:
            responses = {steps[name]['capture']['query_id']: returned(name) for name in evaluation['recall_steps']}
            value['retrieval'], per_query = recall_metrics(bundle, responses, plan['config']['evaluation']['ks'],
                                            plan['config']['protocol'].get('allow_shortlist', True))
            write_json(root / f'results/{evaluation["id"]}_recall.json', responses)
            write_json(root / f'results/{evaluation["id"]}_per_query.json', per_query)
            value['retrieval_slices'] = {}
            for label, subset in bundle.get('retrieval_slices', {}).items():
                ids = set(subset['query_ids'])
                sliced_bundle = {**bundle, 'queries': [q for q in bundle['queries'] if q['query_id'] in ids],
                                 'targets': subset['targets']}
                value['retrieval_slices'][label], _ = recall_metrics(sliced_bundle,
                    {qid: rows for qid, rows in responses.items() if qid in ids}, plan['config']['evaluation']['ks'],
                    plan['config']['protocol'].get('allow_shortlist', True))
        elif evaluation.get('recall_steps'):
            value['retrieval_status'] = 'incomplete_query_responses'
        results.append(value)
        long_rows.extend(metric_rows(value))
    audits = []
    for evaluation in plan['evaluations']:
        path = root/f'execution/runtime/{evaluation["id"]}_audit.json'
        audit = read_json(path) if path.exists() else {'status':'missing','model':evaluation['id']}
        if path.exists() and audit.get('plan_id') != plan['plan_id']:
            raise ValueError('Runtime audit belongs to another plan')
        if audit.get('artifact') and digest(audit['artifact']) != audit['artifact_sha256']:
            raise ValueError('Actual training artifact changed after audit')
        audits.append(audit)
    released = journal.get('resource_status') == 'succeeded'
    formal = not diagnostic and complete and released and bool(journal['formal_transport']) and all(a['status']=='verified' for a in audits)
    status = 'passed' if formal else 'test_only' if not journal['formal_transport'] else (
        'configuration_mismatch' if any(a['status']=='mismatch' for a in audits) else 'runtime_unverified')
    if diagnostic:
        status = 'diagnostic_only' if complete else 'incomplete_workflow'
    if journal.get('resource_status') == 'blocked':
        status = 'cleanup_failed'
    elif complete and not released and journal['formal_transport'] and not diagnostic:
        status = 'resources_unverified'
    parameters_status = 'verified' if all(a['status']=='verified' for a in audits) else (
        'mismatch' if any(a['status']=='mismatch' for a in audits) else 'unverified')
    report = {'format_version': 4, 'execution_profile': 'user_workflow', 'formal': formal,
              'status': status, 'workflow_completed':complete, 'runtime_audits':audits, 'plan_id': plan['plan_id'],
              'workflow_status': 'completed' if complete else 'incomplete',
              'resource_status': journal.get('resource_status', 'legacy_unverified'),
              'model_states': journal.get('models', {}),
              'parameters_status': parameters_status, 'quality_status': 'not_assessed',
              'dataset_fingerprint': bundle['fingerprint'], 'sample_kind': bundle['sample_kind'],
              'experiment': plan['config'].get('experiment', {}),
              'dataset': bundle['source_manifest'].get('dataset'), 'protocol': bundle['protocol'],
              'source_protocol': bundle['source_manifest'].get('protocol'),
              'label_semantics': bundle['source_manifest'].get('label_semantics'),
              'split_description': bundle['source_manifest'].get('split_description'),
              'tasks': plan['tasks'], 'features': plan['features'],
              'task_semantics': bundle.get('task_semantics', {}),
              'sample_counts': {'train_rows': bundle['train_rows'], 'positive_train_rows': bundle.get('positive_train_rows'),
                                'evaluation_rows': len(bundle['observations']), 'queries': len(bundle.get('queries', [])),
                                'catalog_rows': len(bundle.get('items', []))},
              'feature_statistics': bundle.get('feature_statistics', {}),
              'candidate_budget': bundle.get('candidate_budget', {}),
              'candidate_limit': plan['config']['serving'].get('candidate_limit'),
              'entrypoint': plan['config']['serving']['entrypoint'], 'results': results,
              'limitations': ['No configuration selection or bootstrap confidence intervals in this release.',
                              'Integration smoke metrics do not estimate full-dataset model quality.']}
    stem = 'diagnostic_report' if diagnostic else 'report'
    write_json(root / f'results/{stem}.json', report)
    if not diagnostic:
        write_json(root/'results/workflow_conformance.json', {
            'plan_id':plan['plan_id'], 'status':status, 'formal':formal,
            'checks':{
                'public_entrypoint':{'status':'verified' if journal['formal_transport'] else 'test_only',
                                     'entrypoint':plan['config']['serving']['entrypoint']},
                'lifecycle_and_index':{'status':'verified' if complete else 'incomplete',
                                       'required_steps':len(plan['steps'])},
                'effective_parameters':{'status':'verified' if all(a['status']=='verified' for a in audits) else
                                         'failed' if any(a['status']=='mismatch' for a in audits) else 'unverified',
                                         'audits':audits},
                'prediction_coverage':{'status':'verified' if complete else 'incomplete',
                                       'observations':len(bundle['observations']), 'queries':len(bundle.get('queries',[]))},
                'resource_release':{'status':'verified' if released else 'unverified',
                                    'models':journal.get('models', {})}}})
    with (root / ('results/diagnostic_metrics.csv' if diagnostic else 'results/metrics.csv')).open('w') as stream:
        writer = csv.DictWriter(stream, ['experiment', 'recipe', 'model', 'seed', 'slice', 'task', 'metric', 'value']); writer.writeheader(); writer.writerows(long_rows)
    lines = [f'# {plan["name"]}', '', f'Status: {report["status"]}; entrypoint: {report["entrypoint"]}; cohort: {bundle["sample_kind"]}.',
             '', f'Workflow: {report["workflow_status"]}; resources: {report["resource_status"]}; parameters: {parameters_status}; quality: not_assessed.', '',
             f'Dataset: {report["dataset"]}; profile: {report["experiment"].get("profile", "legacy")}; candidate scope: {bundle["protocol"]["candidate_scope"]}.',
             f'Sample counts: {report["sample_counts"]}.', '',
             '| Experiment | Slice | Task | Metric | Value |', '| --- | --- | --- | --- | --- |']
    configured = plan['config']['evaluation'].get('metrics')
    displayed = set(configured) if configured is not None else {'auc', 'ap', 'gauc', 'logloss', 'brier', 'mae', 'rmse'}
    if configured is None:
        displayed.update(f'{name}_at_{k}' for k in plan['config']['evaluation'].get('ks', [])
                         for name in ('recall', 'micro_recall', 'ndcg'))
    for row in long_rows:
        if row['metric'] in displayed:
            value = 'null' if row['value'] is None else f'{row["value"]:.6f}'
            lines.append(f'| {row["experiment"]} | {row["slice"]} | {row["task"]} | {row["metric"]} | {value} |')
    if report['feature_statistics']:
        lines += ['', 'Categorical capacity (uniform-hash estimates, not measured collisions):', '',
                  '| Feature | Cardinality | Model | Buckets | Estimated shared-bucket rate |',
                  '| --- | --- | --- | --- | --- |']
        for feature, stats in report['feature_statistics'].items():
            for model, capacity in stats['models'].items():
                lines.append(f'| {feature} | {stats["cardinality"]} | {model} | {capacity["bucket_size"]} | {capacity["estimated_shared_bucket_rate"]:.2%} |')
    if report['candidate_budget']:
        lines += ['', f'Retrieval candidate budget: {report["candidate_budget"]}.']
    lines += ['', *report['limitations'], '']
    (root / f'results/{stem}.md').write_text('\n'.join(lines))
    return report


def summarize_runs(root, inputs, failed=(), not_executed=()):
    """Keep independent datasets/protocols separate in one reviewable suite report."""
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    runs, rows = [], []
    skipped = {Path(path).resolve() for path in not_executed}
    failures = {Path(path).resolve() for path in failed}
    for directory in inputs:
        directory = Path(directory).resolve()
        if directory in skipped:
            runs.append({'run': directory.name, 'status': 'not_executed', 'suite_status': 'not_executed',
                         'reason': 'previous_resource_barrier_failed', 'formal': False, 'path': str(directory)})
            continue
        path = directory / 'results/report.json'
        if not path.is_file():
            path = directory / 'results/diagnostic_report.json'
        if not path.is_file():
            journal_path = directory / 'execution/journal.json'
            journal = read_json(journal_path) if journal_path.exists() else {}
            status = 'cleanup_failed' if journal.get('resource_status') == 'blocked' else 'missing_report'
            runs.append({'run': directory.name, 'status': status, 'suite_status': 'failed', 'formal': False,
                         'model_states': journal.get('models', {}), 'path': str(directory)})
            continue
        plan = verify_plan(directory); report = read_json(path)
        if report['plan_id'] != plan['plan_id']:
            raise ValueError('Suite report belongs to a different plan')
        context = {'run': directory.name, 'dataset': report.get('dataset', ''),
                   'profile': report.get('experiment', {}).get('profile', 'legacy'),
                   'feature_view': report.get('experiment', {}).get('feature_view', 'custom'),
                   'candidate_scope': report.get('protocol', plan['config']['protocol'])['candidate_scope'],
                   'formal': report['formal'], 'status': report['status'],
                   'suite_status': 'failed' if directory in failures or not report.get('workflow_completed') or report.get('resource_status') == 'blocked' else 'completed'}
        runs.append({**context, 'path': str(path), 'sample_counts': report.get('sample_counts', {}),
                     'resource_status': report.get('resource_status', 'legacy_unverified'),
                     'model_states': report.get('model_states', {})})
        for result in report['results']:
            for row in metric_rows(result):
                rows.append({**context, **{k:row[k] for k in ('model', 'seed', 'slice', 'task', 'metric', 'value')}})
    summary = {'runs': runs, 'metrics': rows, 'quality_status': 'not_assessed',
               'missing_reports': sum(r['status'] == 'missing_report' for r in runs),
               'failed_runs': sum(r['suite_status'] == 'failed' for r in runs),
               'not_executed_runs': sum(r['suite_status'] == 'not_executed' for r in runs)}
    write_json(root / 'summary.json', summary)
    with (root / 'metrics.csv').open('w') as stream:
        columns = ['run', 'dataset', 'profile', 'feature_view', 'candidate_scope', 'formal', 'status', 'suite_status',
                   'model', 'seed', 'slice', 'task', 'metric', 'value']
        writer = csv.DictWriter(stream, columns); writer.writeheader(); writer.writerows(rows)
    lines = ['# Model quality suite', '', 'Metrics use each dataset\'s own labels and candidate protocol. No quality threshold is assessed.', '',
             '| Run | Dataset | Profile | Candidate scope | Status | Suite status | Formal |', '| --- | --- | --- | --- | --- | --- | --- |']
    for run in runs:
        lines.append(f'| {run["run"]} | {run.get("dataset", "unknown")} | {run.get("profile", "unknown")} | {run.get("candidate_scope", "unknown")} | {run["status"]} | {run["suite_status"]} | {run["formal"]} |')
    lines += ['', '| Run | Model | Seed | Slice | Task | Metric | Value |', '| --- | --- | --- | --- | --- | --- | --- |']
    displayed = {'auc', 'gauc', 'ap', 'logloss', 'brier', 'rmse', 'mae'}
    for row in rows:
        if row['metric'] in displayed or row['metric'].startswith(('recall_at_', 'ndcg_at_', 'micro_recall_at_')):
            value = 'null' if row['value'] is None else f'{row["value"]:.6f}'
            lines.append(f'| {row["run"]} | {row["model"]} | {row["seed"]} | {row["slice"]} | {row["task"]} | {row["metric"]} | {value} |')
    (root / 'summary.md').write_text('\n'.join(lines) + '\n')
    return summary
