"""Current results only: one row per dataset/model/task, two quality metrics."""
from pathlib import Path

from .common import write_json


def number(value):
    return 'N/A' if value is None else f'{value:.4f}'


def write_report(root, results, full_eval):
    root = Path(root)
    write_json(root / 'results.json', {'evaluation': 'full' if full_eval else 'sampled', 'results': results})
    lines = ['# Model quality', '', f'Evaluation: {"full valid" if full_eval else "fixed valid sample"}; training: full train.',
             'MovieLens: rating preference; KuaiRand: mixed standard/random click exposures; KuaiRec: observed engagement.', '']
    for kind in ('ranking', 'retrieval'):
        selected = [result for result in results if result['kind'] == kind]
        if not selected:
            continue
        if kind == 'ranking':
            lines.extend(['| Dataset / task | Model | AUC ↑ | LogLoss ↓ | Rows | Seconds | Status |',
                          '| --- | --- | --- | --- | --- | --- | --- |'])
        else:
            lines.extend(['| Dataset / candidates | Model | Recall@50 ↑ | NDCG@50 ↑ | Queries | Seconds | Status |',
                          '| --- | --- | --- | --- | --- | --- | --- |'])
        for result in selected:
            metrics = result.get('metrics', {})
            if kind == 'retrieval':
                metrics = {'retrieval': metrics}
            elif not metrics:
                metrics = {task: {} for task in result['tasks']}
            for task, values in metrics.items():
                label = result['candidate_scope'] if kind == 'retrieval' else task
                primary, secondary, count = ('recall_at_50', 'ndcg_at_50', 'queries') if kind == 'retrieval' else ('auc', 'logloss', 'rows')
                lines.append(f'| {result["dataset"]} / {label} | {result["model"]} | {number(values.get(primary))} | '
                             f'{number(values.get(secondary))} | {values.get(count, "—")} | '
                             f'{result.get("seconds", 0):.1f} | {result["status"]} |')
        lines.append('')
    for result in results:
        notes = []
        if result['kind'] == 'ranking':
            for task, values in result.get('metrics', {}).items():
                notes.append(f'{task} positive rate {values["positive_rate"]:.1%}')
                if values.get('auc_reason'):
                    notes.append(f'{task}: AUC N/A ({values["auc_reason"]})')
        elif result.get('metrics'):
            values = result['metrics']
            notes.append(f'{values["positive_queries"]}/{values["queries"]} positive queries')
            if values['empty_lists'] or values['short_lists']:
                notes.append(f'{values["empty_lists"]} empty lists, {values["short_lists"]} short lists')
        if result.get('error'):
            notes.append(result['error'].replace('\n', ' ')[:400])
        if notes:
            lines.append(f'- {result["dataset"]}/{result["model"]}: ' + '; '.join(notes))
    (root / 'summary.md').write_text('\n'.join(lines) + '\n')
