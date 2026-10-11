"""Four quality metrics, with explicit prediction identity and coverage checks."""
import math

import numpy as np

from .common import binary, finite
from .config import K


def binary_metrics(labels, probabilities):
    labels = binary(labels)
    scores = np.asarray(probabilities, dtype=float)
    if not len(labels) or scores.shape != labels.shape or not np.isfinite(scores).all():
        raise ValueError('Predictions must be finite and aligned with nonempty labels')
    if (scores < 0).any() or (scores > 1).any():
        raise ValueError('Binary predictions must be probabilities in [0, 1]')
    # Equal scores share a rank: positive/negative ties contribute half a win.
    order = np.argsort(scores, kind='stable')
    starts = np.flatnonzero(np.r_[True, scores[order][1:] != scores[order][:-1]])
    positive = np.add.reduceat(labels[order].astype(float), starts)
    negative = np.add.reduceat(1.0 - labels[order], starts)
    p, n = positive.sum(), negative.sum()
    auc = float(np.sum(positive * (np.cumsum(negative) - .5 * negative)) / (p * n)) if p and n else None
    clipped = np.clip(scores, 1e-15, 1 - 1e-15)
    logloss = float(-np.mean(labels * np.log(clipped) + (1 - labels) * np.log1p(-clipped)))
    return {'auc': auc, 'logloss': logloss, 'rows': len(labels),
            'positive_rate': float(labels.mean()), 'auc_reason': None if p and n else 'single_class'}


def ranking_metrics(observations, predictions, tasks, outputs):
    expected = list(observations.row_id)
    ids = [row.get('row_id') for row in predictions]
    if len(set(expected)) != len(expected) or len(set(ids)) != len(ids) or set(ids) != set(expected):
        raise ValueError('Predictions must exactly cover unique observation IDs')
    by_id = {row['row_id']: row for row in predictions}
    return {task.id: binary_metrics(observations[task.column],
                                   [by_id[row_id].get(outputs[task.id]) for row_id in expected])
            for task in tasks}


def retrieval_metrics(queries, responses, catalog):
    expected = {query['query_id']: query for query in queries}
    if len(expected) != len(queries) or set(responses) != set(expected):
        raise ValueError('Missing or unexpected query responses, including empty lists')
    recalls, ndcgs = [], []
    empty, short = 0, 0
    for query_id, query in expected.items():
        rows = responses[query_id]
        items = [row.get('item_id') for row in rows]
        seen = set(query['seen'])
        pool = catalog & set(query['allowed']) if 'allowed' in query else catalog
        if len(set(items)) != len(items) or any(item not in pool or item in seen for item in items):
            raise ValueError('Duplicate, ineligible or seen recommendations')
        if any(row.get('query_id') != query_id or not finite(row.get('score')) for row in rows):
            raise ValueError('Invalid query identity or score')
        if len(rows) > K or any(rows[i]['score'] < rows[i + 1]['score'] for i in range(len(rows) - 1)):
            raise ValueError('Recommendations must be score-ordered Top-K')
        empty += not rows
        short += len(rows) < min(K, len(pool - seen))
        targets = set(query['targets'])
        if not targets:
            continue
        hits = [int(item in targets) for item in items]
        recalls.append(sum(hits) / len(targets))
        ideal = sum(1 / math.log2(i + 2) for i in range(min(K, len(targets))))
        ndcgs.append(sum(hit / math.log2(i + 2) for i, hit in enumerate(hits)) / ideal)
    return {'recall_at_50': float(np.mean(recalls)) if recalls else None,
            'ndcg_at_50': float(np.mean(ndcgs)) if ndcgs else None,
            'queries': len(queries), 'positive_queries': len(recalls),
            'zero_positive_queries': len(queries) - len(recalls), 'empty_lists': int(empty), 'short_lists': int(short)}
