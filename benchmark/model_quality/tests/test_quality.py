"""Hand-computed metrics, leakage controls, and executable CLI contracts."""
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from benchmark.model_quality.common import load_split, stable_id
import pyarrow as pa
import pyarrow.parquet as pq
from benchmark.model_quality.metrics import BinaryMetrics, grouped_auc, scalar_metrics
from benchmark.model_quality.prepare import batch_collisions, past_counts, write_frame
from benchmark.model_quality.prepare_kuairand import prepare as prepare_rand
from benchmark.model_quality.prepare_kuairec import prepare as prepare_rec
from benchmark.model_quality.user_workflow.configuration import read_dataset_definition


def preparation_definition():
    path = Path(__file__).resolve().parents[1] / 'configs/datasets/kuairand.yaml'
    return read_dataset_definition(path)[0]


class MetricsTest(unittest.TestCase):

    def test_shared_gauc_matches_an_independent_weighted_reference(self):
        labels = [0, 1, 0, 1, 1, 1]
        scores = [0, 1, 1, 0, 0, 0]
        users = ['a', 'a', 'b', 'b', 'b', 'c']
        expected = (2*roc_auc_score(labels[:2], scores[:2]) + 3*roc_auc_score(labels[2:5], scores[2:5]))/5
        shared = grouped_auc(labels, scores, users)
        self.assertAlmostEqual(shared['gauc'], expected)
        self.assertAlmostEqual(shared['gauc_row_coverage'], 5/6)
        observations = [{'row_id': str(i), 'label': label, 'user_id': user} for i, (label, user) in enumerate(zip(labels, users))]
        predictions = [{'row_id': str(i), 'score': score} for i, score in enumerate(scores)]
        scalar = scalar_metrics(observations, predictions, [{'id':'click','column':'label','type':'binary'}], {'click':'score'}, False, 'user_id')
        self.assertEqual(scalar['click']['gauc'], shared['gauc'])
        self.assertEqual(scalar['click']['gauc_row_coverage'], shared['gauc_row_coverage'])
        with self.assertRaises(ValueError):
            grouped_auc(labels, scores, users[:-1]+[None])

    def test_auc_ap_ties_and_weights_match_independent_reference(self):
        rng = np.random.default_rng(7)
        for _ in range(20):
            y, scores, weights = rng.integers(2, size=100), rng.integers(5, size=100) / 4, rng.integers(4, size=100)
            actual = BinaryMetrics(y, scores).compute(weights)
            self.assertAlmostEqual(actual['auc'], roc_auc_score(y, scores, sample_weight=weights))
            self.assertEqual(actual['auc'], BinaryMetrics(y, scores).auc(weights))
            self.assertAlmostEqual(actual['ap'], average_precision_score(y, scores, sample_weight=weights))
        tied = BinaryMetrics([1, 0], [.3, .3]).compute()
        self.assertEqual(tied['auc'], .5)
        self.assertEqual(tied['ap'], .5)

    def test_degenerate_labels_and_raw_scores_are_explicit(self):
        self.assertEqual(BinaryMetrics([0, 0], [.1, .2]).compute()['auc_reason'], 'single_class')
        self.assertEqual(BinaryMetrics([], []).compute()['auc_reason'], 'empty')
        with self.assertRaises(ValueError):
            BinaryMetrics([0, .5], [.1, .2])
        with self.assertRaises(ValueError):
            BinaryMetrics([0, 1], [-1, 2]).compute()
        self.assertEqual(BinaryMetrics([0, 1], [-1, 2]).compute(probability=False)['auc'], 1)

    def test_predictions_align_by_identity_and_fail_closed(self):
        observations = [{'row_id': 'a', 'label': 0}, {'row_id': 'b', 'label': 1}]
        prediction = [{'row_id': 'b', 'score': .9}, {'row_id': 'a', 'score': .1}]
        tasks = [{'id': 'click', 'column': 'label', 'type': 'binary'}]
        actual = scalar_metrics(observations, prediction, tasks, {'click': 'score'}, True)
        self.assertEqual(actual['click']['auc'], 1)
        for invalid in (prediction[:1], prediction + prediction[:1],
                        [dict(r, score=float('nan')) for r in prediction]):
            with self.assertRaises(ValueError):
                scalar_metrics(observations, invalid, tasks, {'click': 'score'}, True)

    def test_gauc_single_class_group_remains_in_coverage(self):
        report = grouped_auc([0, 1, 1], [.1, .9, .9], ['a', 'a', 'b'])
        self.assertEqual(report['gauc'], 1)
        self.assertEqual(report['gauc_users'], 1)
        self.assertAlmostEqual(report['gauc_exposure_coverage'], 2 / 3)

    def test_same_timestamp_feedback_is_not_in_history(self):
        train = pd.DataFrame({'user_id': ['u'] * 3, 'time_ms': [1, 1, 2], 'label': [1, 0, 1]})
        config = preparation_definition()
        statistic = next(s for s in config['preparation']['statistics'] if s['key'] == 'user_id')
        history = past_counts(train, statistic, 'label', config['preparation']['time_key'])
        np.testing.assert_allclose(history.user_show_count, [0, np.log1p(2)])
        np.testing.assert_allclose(history.user_positive_count, [0, np.log1p(1)])

    def test_batch_collision_diagnostic(self):
        stats = batch_collisions(pd.DataFrame({'user_id': ['u', 'u', 'v'], 'video_id': ['a', 'b', 'a']}), preparation_definition())
        self.assertEqual(stats['off_diagonal_pairs'], 6)
        self.assertEqual(stats['same_item_pair_fraction'], 1/3)
        self.assertEqual(stats['same_user_pair_fraction'], 1/3)

    def test_parquet_schema_is_compatible_with_fg_including_empty_tags(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'features.parquet'
            write_frame(path, pd.DataFrame({'user_id': pd.Series(['u'], dtype='str'),
                'tag': [[]], 'user_show_count': [1], 'label': [1]}), preparation_definition())
            schema = pq.read_schema(path)
            self.assertEqual(schema.field('user_id').type, pa.string())
            self.assertEqual(schema.field('tag').type, pa.list_(pa.string()))
            self.assertEqual(schema.field('user_show_count').type, pa.float32())
            self.assertEqual(schema.field('label').type, pa.int32())


def rand_fixture(root):
    def event(user, item, date, timestamp, label):
        return {'user_id': str(user), 'video_id': str(item), 'tab': '1', 'date': date,
                'time_ms': timestamp, 'is_click': label, 'long_view': label, 'is_rand': 0}
    train = pd.DataFrame([event('u', 'a', 20220421, 1, 1), event('u', 'b', 20220421, 2, 0),
                          event('v', 'b', 20220421, 2, 1), event('v', 'a', 20220421, 3, 0)])
    future = pd.DataFrame([event('u', 'a', 20220422, 10, 1), event('u', 'b', 20220422, 11, 0),
                           event('u', 'a', 20220429, 20, 1), event('u', 'b', 20220429, 21, 0)])
    train.to_csv(root / 'log_standard_4_08_to_4_21_pure.csv', index=False)
    future.to_csv(root / 'log_standard_4_22_to_5_08_pure.csv', index=False)
    future.assign(is_rand=1).to_csv(root / 'log_random_4_22_to_5_08_pure.csv', index=False)
    pd.DataFrame({'video_id': ['a', 'b', 'c'], 'author_id': ['x', 'y', 'z'], 'tag': ['1', '2', '3'],
                  'video_duration': [1000, 2000, 3000], 'upload_dt': ['2022-04-01', '2022-04-01', '2022-05-01']}).to_csv(root / 'video_features_basic_pure.csv', index=False)


class PreparationTest(unittest.TestCase):


    def test_real_exposure_protocol_and_immutable_preparation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rand_fixture(root)
            data = root / 'prepared'
            manifest = prepare_rand(root, data)
            train, valid, test = [load_split(data, s) for s in ('train', 'valid', 'test')]
            self.assertLess(train.time_ms.max(), valid.time_ms.min())
            self.assertLess(valid.time_ms.max(), test.time_ms.min())
            self.assertEqual(len(test), 4)
            self.assertTrue((pd.read_parquet(data / 'recall_train.parquet').label == 1).all())
            self.assertFalse(pd.read_parquet(data / 'items.parquet').set_index('video_id').loc['c', 'eligible'])
            self.assertAlmostEqual(test.loc[test.user_id == 'u', 'user_positive_count'].iloc[0], np.log1p(1), places=6)
            with self.assertRaises(FileExistsError):
                prepare_rand(root, data)

    def test_logged_date_boundary_overlap_is_purged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rand_fixture(root)
            path = root / 'log_standard_4_08_to_4_21_pure.csv'
            train = pd.read_csv(path)
            extra = train.iloc[:1].assign(time_ms=11)
            pd.concat([train, extra]).to_csv(path, index=False)
            manifest = prepare_rand(root, root / 'prepared')
            self.assertEqual(manifest['temporal_boundary_audit']['purged_train_rows_before_cohort'], 1)
            self.assertLess(load_split(root / 'prepared', 'train').time_ms.max(), 10)

    def test_pair_holdout_never_turns_missing_pairs_into_negatives(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [{'user_id': u, 'video_id': v, 'timestamp': v + 1, 'watch_ratio': 3 if v % 3 == 0 else .5,
                     'video_duration': 1000} for u in range(3) for v in range(100) if not (u == 0 and v == 99)]
            # Repeated pair with conflicting future feedback must not cross splits/change its label.
            rows.append(dict(rows[0], timestamp=999, watch_ratio=0))
            rows[1]['timestamp'] = np.nan
            pd.DataFrame(rows).to_csv(root / 'small_matrix.csv', index=False)
            pd.DataFrame({'video_id': range(100), 'feat': ['[1,2]'] * 100}).to_csv(root / 'item_categories.csv', index=False)
            data = root / 'prepared'
            manifest = prepare_rec(root, data)
            train, valid, test = [load_split(data, s) for s in ('train', 'valid', 'test')]
            self.assertEqual(len(train) + len(valid) + len(test), 299)
            self.assertEqual(manifest['missing_timestamps'], 1)
            self.assertEqual(pd.concat([train, valid, test]).timestamp_missing.sum(), 1)
            self.assertFalse(set(train.row_id) & set(test.row_id))
            first = pd.concat([train, valid, test]).set_index('row_id').loc[stable_id('kuairec-pair', 0, 0)]
            self.assertEqual(first.label, 1)
            items = pd.read_parquet(data / 'items.parquet')


if __name__ == '__main__':
    unittest.main()
