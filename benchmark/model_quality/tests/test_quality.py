"""Hand-computed metrics, leakage controls, and executable CLI contracts."""
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

from benchmark.model_quality.common import load_split, stable_id
import pyarrow as pa
import pyarrow.parquet as pq
from benchmark.model_quality.metrics import binary_metrics, ranking_metrics
from benchmark.model_quality.config import Task
from benchmark.model_quality.prepare import past_counts, write_frame
from benchmark.model_quality.prepare_kuairand import prepare as prepare_rand
from benchmark.model_quality.prepare_kuairec import prepare as prepare_rec
from benchmark.model_quality.config import read_dataset_definition


def preparation_definition():
    path = Path(__file__).resolve().parents[1] / 'configs/datasets/kuairand.yaml'
    return read_dataset_definition(path)[0]


class MetricsTest(unittest.TestCase):

    def test_auc_logloss_match_independent_reference(self):
        rng = np.random.default_rng(7)
        for _ in range(20):
            labels = rng.integers(2, size=100)
            scores = rng.integers(5, size=100) / 4
            actual = binary_metrics(labels, scores)
            self.assertAlmostEqual(actual['auc'], roc_auc_score(labels, scores))
            clipped = np.clip(scores, 1e-15, 1-1e-15)
            self.assertAlmostEqual(actual['logloss'], log_loss(labels, clipped))
        self.assertEqual(binary_metrics([1, 0], [.3, .3])['auc'], .5)

    def test_degenerate_and_invalid_inputs(self):
        self.assertEqual(binary_metrics([0, 0], [.1, .2])['auc_reason'], 'single_class')
        for labels, scores in [([], []), ([0, .5], [.1, .2]), ([0, 1], [-1, 2]), ([0, 1], [np.nan, .5])]:
            with self.assertRaises(ValueError):
                binary_metrics(labels, scores)

    def test_predictions_align_by_identity(self):
        observations = pd.DataFrame([{'row_id': 'a', 'label': 0}, {'row_id': 'b', 'label': 1}])
        prediction = [{'row_id': 'b', 'probs': .9}, {'row_id': 'a', 'probs': .1}]
        tasks = [Task('click', 'label')]
        self.assertEqual(ranking_metrics(observations, prediction, tasks, {'click': 'probs'})['click']['auc'], 1)
        for invalid in (prediction[:1], prediction + prediction[:1]):
            with self.assertRaises(ValueError):
                ranking_metrics(observations, invalid, tasks, {'click': 'probs'})

    def test_same_timestamp_feedback_is_not_in_history(self):
        train = pd.DataFrame({'user_id': ['u'] * 3, 'time_ms': [1, 1, 2], 'label': [1, 0, 1]})
        config = preparation_definition()
        statistic = next(s for s in config['preparation']['statistics'] if s['key'] == 'user_id')
        history = past_counts(train, statistic, 'label', config['preparation']['time_key'])
        np.testing.assert_allclose(history.user_show_count, [0, np.log1p(2)])
        np.testing.assert_allclose(history.user_positive_count, [0, np.log1p(1)])

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

    def test_kuairec_heldout_feedback_cannot_change_training_or_features(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [{'user_id': str(u), 'video_id': str(v), 'timestamp': np.nan if u % 7 == 0 else 1,
                     'watch_ratio': 3 if (u + v) % 3 == 0 else .5, 'video_duration': 1000 + u}
                    for u in range(20) for v in range(30)]
            raw = pd.DataFrame(rows)
            raw.to_csv(root / 'small_matrix.csv', index=False)
            pd.DataFrame({'video_id': range(30), 'feat': ['[1,2]'] * 30}).to_csv(root / 'item_categories.csv', index=False)
            original = root / 'original'
            prepare_rec(root, original)
            train_pairs = set(load_split(original, 'train')[['user_id', 'video_id']].itertuples(index=False, name=None))
            heldout = [pair not in train_pairs for pair in raw[['user_id', 'video_id']].itertuples(index=False, name=None)]
            raw.loc[heldout, 'watch_ratio'] = 100.
            raw.to_csv(root / 'small_matrix.csv', index=False)
            changed = root / 'changed'
            prepare_rec(root, changed)
            pd.testing.assert_frame_equal(load_split(original, 'train'), load_split(changed, 'train'))
            pd.testing.assert_frame_equal(pd.read_parquet(original / 'items.parquet'),
                                          pd.read_parquet(changed / 'items.parquet'))
            for split in ('valid', 'test'):
                pd.testing.assert_frame_equal(load_split(original, split).drop(columns='label'),
                                              load_split(changed, split).drop(columns='label'))
                pd.testing.assert_frame_equal(pd.read_parquet(original / f'queries_{split}.parquet'),
                                              pd.read_parquet(changed / f'queries_{split}.parquet'))


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
