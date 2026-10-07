"""Exposure metrics with exact row alignment, tied-score AUC/AP and coverage."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .common import binary, finite


class BinaryMetrics:
    """Sort once for tied-score AUC and average precision."""
    def __init__(self, labels, scores):
        self.y = binary(labels).astype(float)
        self.s = np.asarray(scores, dtype=float)
        if self.s.shape != self.y.shape or not np.isfinite(self.s).all():
            raise ValueError('Scores must be finite scalars aligned with labels')
        self.order = np.argsort(self.s, kind='stable')
        sorted_s = self.s[self.order]
        self.starts = np.flatnonzero(np.r_[True, sorted_s[1:] != sorted_s[:-1]]) if len(sorted_s) else np.array([], dtype=int)

    def compute(self, weights=None, probability=True):
        w = np.ones(len(self.y)) if weights is None else np.asarray(weights, dtype=float)
        if w.shape != self.y.shape or not np.isfinite(w).all() or (w < 0).any():
            raise ValueError('Weights must be finite, nonnegative and aligned')
        total = float(w.sum())
        result = {'rows': len(self.y), 'weight': total, 'positives': float(w @ self.y), 'auc': None, 'ap': None,
                  'logloss': None, 'brier': None, 'auc_reason': None}
        if total == 0:
            result['auc_reason'] = 'empty'
            return result
        yp = (w * self.y)[self.order]
        yn = (w * (1 - self.y))[self.order]
        pos = np.add.reduceat(yp, self.starts)
        neg = np.add.reduceat(yn, self.starts)
        p, n = pos.sum(), neg.sum()
        if p > 0 and n > 0:
            result['auc'] = float(np.sum(pos * (np.cumsum(neg) - 0.5 * neg)) / (p * n))
        else:
            result['auc_reason'] = 'single_class'
        # Average precision: all equal scores enter the same precision-recall step.
        if p > 0:
            cp, cn = np.cumsum(pos[::-1]), np.cumsum(neg[::-1])
            result['ap'] = float(np.sum(pos[::-1] / p * np.divide(cp, cp + cn, out=np.zeros_like(cp), where=cp + cn > 0)))
        if probability:
            if (self.s < 0).any() or (self.s > 1).any():
                raise ValueError('Probability metrics require scores in [0,1]; declare probability=False for inner products')
            clipped = np.clip(self.s, 1e-15, 1 - 1e-15)
            result['logloss'] = float(-w @ (self.y * np.log(clipped) + (1 - self.y) * np.log1p(-clipped)) / total)
            result['brier'] = float(w @ (self.s - self.y) ** 2 / total)
        return result

    def auc(self, weights=None):
        """AUC-only path for grouped metrics, avoiding unused calibration work."""
        w = np.ones(len(self.y)) if weights is None else np.asarray(weights, dtype=float)
        if w.shape != self.y.shape or not np.isfinite(w).all() or (w < 0).any():
            raise ValueError('Weights must be finite, nonnegative and aligned')
        if not len(self.y):
            return None
        pos = np.add.reduceat((w * self.y)[self.order], self.starts)
        neg = np.add.reduceat((w * (1 - self.y))[self.order], self.starts)
        p, n = pos.sum(), neg.sum()
        return float(np.sum(pos * (np.cumsum(neg) - 0.5 * neg)) / (p * n)) if p > 0 and n > 0 else None


def grouped_auc(labels, scores, groups):
    """Exposure-weighted GAUC; single-class groups remain in coverage's denominator."""
    labels, scores = np.asarray(labels), np.asarray(scores)
    groups = pd.Series(groups).reset_index(drop=True)
    if len(groups) != len(labels) or groups.isna().any():
        raise ValueError('GAUC groups must be non-null and aligned')
    aucs, sizes = [], []
    for indices in groups.groupby(groups, sort=False).indices.values():
        y = labels[indices]
        auc = BinaryMetrics(y, scores[indices]).auc()
        if auc is not None:
            aucs.append(auc)
            sizes.append(len(indices))
    coverage = float(sum(sizes) / len(labels)) if len(labels) else 0
    return {'users': int(groups.nunique()), 'gauc_users': len(aucs),
            'gauc_exposure_coverage': coverage, 'gauc_row_coverage': coverage,
            'gauc': float(np.average(aucs, weights=sizes)) if aucs else None,
            'user_macro_auc': float(np.mean(aucs)) if aucs else None}


def scalar_metrics(observations, predictions, tasks, outputs, probability, group=None):
    identities = [r.get('row_id') for r in predictions]
    expected = [r['row_id'] for r in observations]
    if len(set(identities)) != len(identities) or set(identities) != set(expected):
        raise ValueError('Scalar outputs must exactly cover frozen observation identities')
    scores = {r['row_id']: r for r in predictions}; metrics = {}
    for task in tasks:
        y = np.asarray([r[task['column']] for r in observations], dtype=float)
        s = np.asarray([scores[r['row_id']].get(outputs[task['id']]) for r in observations], dtype=float)
        if not np.isfinite(s).all():
            raise ValueError('Missing/nonfinite task predictions')
        if task['type'] == 'binary':
            result = BinaryMetrics(y, s).compute(probability=probability)
            if group:
                grouped = grouped_auc(y, s, [r.get(group) for r in observations])
                result.update(gauc=grouped['gauc'], gauc_row_coverage=grouped['gauc_row_coverage'])
        else:
            result = {'rows': len(y), 'rmse': float(np.sqrt(np.mean((y-s)**2))),
                      'mae': float(np.mean(np.abs(y-s)))}
        metrics[task['id']] = result
    return metrics


def recall_metrics(bundle, responses, ks, allow_shortlist=True):
    expected = {q['query_id']: q for q in bundle['queries']}
    if set(responses) != set(expected):
        raise ValueError('Missing query responses, including empty-list responses')
    catalog = {r['item_id'] for r in bundle['items']}
    metrics = {}; per_query = []
    for query_id, query in expected.items():
        rows = responses[query_id]; items = [r.get('item_id') for r in rows]
        candidate_pool = catalog & set(query['candidates']) if 'candidates' in query else catalog
        if len(set(items)) != len(items) or any(i not in candidate_pool or i in query['seen'] for i in items):
            raise ValueError('Duplicate/ineligible/seen recommendations')
        if any(r.get('query_id') != query_id or not finite(r.get('score')) for r in rows):
            raise ValueError('Invalid query identity or score')
        if any(rows[i]['score'] < rows[i+1]['score'] for i in range(len(rows)-1)):
            raise ValueError('Public recommendations are not ordered by declared score')
        if len(items) > max(ks):
            raise ValueError('Returned list exceeds public LIMIT')
        eligible_count = len(candidate_pool - set(query['seen']))
        if not allow_shortlist and len(items) < min(max(ks), eligible_count):
            raise ValueError('Shortlist violates declared full-K return contract')
        targets = set(bundle['targets'][query_id])
        legal_targets = targets & (candidate_pool - set(query['seen']))
        row = {'query_id': query_id, 'targets': len(targets), 'unretrievable_targets': len(targets-legal_targets),
               'returned': len(items), 'shortlist': len(items) < min(max(ks), eligible_count)}
        for k in ks:
            hits = [int(item in targets) for item in items[:k]]
            ideal = sum(1 / math.log2(i+2) for i in range(min(k, len(targets))))
            row[f'hits_at_{k}'] = sum(hits)
            row[f'recall_at_{k}'] = sum(hits)/len(targets) if targets else None
            row[f'ndcg_at_{k}'] = sum(h/math.log2(i+2) for i, h in enumerate(hits))/ideal if ideal else None
        per_query.append(row)
    positive = [r for r in per_query if r['targets']]
    total = sum(r['targets'] for r in per_query)
    metrics.update(queries=len(per_query), positive_queries=len(positive), zero_positive_queries=len(per_query)-len(positive),
                   empty_lists=sum(r['returned'] == 0 for r in per_query),
                   shortlist_rate=sum(r['shortlist'] for r in per_query)/len(per_query),
                   unretrievable_target_rate=sum(r['unretrievable_targets'] for r in per_query)/total if total else None,
                   item_coverage=len({r['item_id'] for rows in responses.values() for r in rows})/len(catalog))
    for k in ks:
        metrics[f'recall_at_{k}'] = float(np.mean([r[f'recall_at_{k}'] for r in positive])) if positive else None
        metrics[f'micro_recall_at_{k}'] = sum(r[f'hits_at_{k}'] for r in positive)/total if total else None
        metrics[f'ndcg_at_{k}'] = float(np.mean([r[f'ndcg_at_{k}'] for r in positive])) if positive else None
    return metrics, per_query


