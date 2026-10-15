"""Pure-ranking data, duplicate-safe splits, and dataset/model selection."""
from pathlib import Path
from contextlib import redirect_stderr
import io
import copy
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from benchmark.model_quality import config, prepare_criteo
from benchmark.model_quality import __main__ as runner
from benchmark.model_quality.common import load_split, write_json
from benchmark.model_quality.data import load_dataset, check_prepared
from benchmark.model_quality.prepare import preparation_version


def raw_frame(rows=400):
    frame = pd.DataFrame({'target': [str(i % 2) for i in range(rows)]})
    for i in range(13):
        frame[f'intCol_{i}'] = [str(j + i) for j in range(rows)]
    for i in range(26):
        frame[f'catCol_{i}'] = [f'{j:08x}' for j in range(rows)]
    frame.loc[0, 'catCol_0'] = '00000001'
    frame.loc[0, 'catCol_1'] = '00000000'
    frame.loc[0, 'catCol_2'] = 'nan'
    frame.loc[0, 'catCol_3'] = ''
    frame.loc[0, 'intCol_0'] = ''
    return frame


class CriteoTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.definition = copy.deepcopy(config.dataset_definition('criteo1m'))
        self.raw = self.root / 'raw'
        self.raw.mkdir()
        self.filename = self.raw / self.definition['source']['files']['observations']

    def prepare(self, frame, output):
        frame.to_csv(self.filename, index=False)
        self.definition['preparation']['parameters']['expected_rows'] = len(frame)
        with patch.object(prepare_criteo, 'load_preparation_config', return_value=(self.definition, {})):
            return prepare_criteo.prepare(self.raw, output)

    def test_duplicate_features_stay_together_without_labels_in_split(self):
        frame = raw_frame()
        frame = pd.concat([frame, frame.iloc[:10].assign(target=lambda f: 1-f.target.astype(int))], ignore_index=True)
        first = self.root / 'first'
        manifest = self.prepare(frame, first)
        splits = [load_split(first, name).assign(split=name) for name in ('train', 'valid', 'test')]
        combined = pd.concat(splits)
        self.assertEqual(len(combined), len(frame))
        self.assertEqual(combined.row_id.nunique(), len(frame))
        self.assertTrue(combined.groupby('feature_id').split.nunique().eq(1).all())
        changed = self.root / 'changed'
        self.prepare(frame.assign(target=lambda f: 1-f.target.astype(int)), changed)
        for split in ('train', 'valid', 'test'):
            pd.testing.assert_frame_equal(load_split(first, split).drop(columns='label'),
                                          load_split(changed, split).drop(columns='label'))
        self.assertEqual(set(manifest['training']), {'ranking', 'trees'})
        self.assertFalse((first / 'items.parquet').exists())
        self.assertFalse((first / 'queries_valid.parquet').exists())
        with self.assertRaises(FileExistsError):
            self.prepare(frame, first)

    def test_raw_categories_types_and_ranking_only_loading(self):
        output = self.root / 'prepared/criteo1m' / preparation_version('criteo1m')
        manifest = self.prepare(raw_frame(), output)
        manifest['preparation_version'] = preparation_version('criteo1m')
        write_json(output / 'manifest.json', manifest)
        with patch('benchmark.model_quality.data.dataset_definition', return_value=self.definition):
            dataset = load_dataset('criteo1m', self.root, full_eval=True)
            check_prepared(output, 'criteo1m')
        self.assertTrue(dataset.items.empty and dataset.queries.empty)
        self.assertEqual(len(dataset.ranking_sample()), manifest['audit']['valid']['rows'])
        all_rows = pd.concat([load_split(output, split) for split in ('train', 'valid', 'test')])
        special = all_rows.loc[all_rows.c2.eq('00000000')].iloc[0]
        self.assertEqual((special.c1, special.c3, special.c4), ('00000001', 'nan', '__MISSING__'))
        self.assertEqual(special.i1, 0)
        self.assertEqual(str(all_rows.i1.dtype), 'float32')
        self.assertEqual(str(all_rows.label.dtype), 'int32')
        for view, count in (('ranking', 39), ('trees', 13)):
            train = pd.read_parquet(output / manifest['training'][view]['file'])
            self.assertEqual(len(train.columns), count+1)
            self.assertNotIn('row_id', train)
            self.assertNotIn('feature_id', train)

    def test_bad_labels_numbers_or_schema_are_rejected(self):
        for column, value in [('target', '2'), ('intCol_0', 'bad'), ('intCol_1', 'inf')]:
            frame = raw_frame()
            frame.loc[0, column] = value
            with self.subTest(column=column), self.assertRaises(ValueError):
                self.prepare(frame, self.root / column)
        with self.assertRaisesRegex(ValueError, 'columns or row count'):
            self.prepare(raw_frame().drop(columns='catCol_0'), self.root / 'missing')

    def test_explicit_unsupported_models_fail_before_cluster_access(self):
        for model in ('dssm', 'mmoe'):
            with patch('sys.argv', ['benchmark', '--dataset', 'criteo1m', '--model', model]), \
                 patch.object(runner, 'environment') as connect, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    runner.arguments()
                self.assertEqual(error.exception.code, 2)
                connect.assert_not_called()

    def test_supported_models_and_feature_views(self):
        self.assertEqual(config.applicable_models('criteo1m', 'all'),
                         ['wide_and_deep', 'deepfm', 'rocket_launching', 'catboost', 'lightgbm', 'xgboost'])
        for model in config.applicable_models('criteo1m', 'all'):
            features, tasks = config.view_fields(self.definition, model)
            self.assertEqual(len(features), 13 if model in ('lightgbm', 'xgboost') else 39)
            self.assertEqual([(t.id, t.column) for t in tasks], [('click', 'label')])
        for model in ('dssm', 'mmoe'):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                config.applicable_models('criteo1m', model)
        self.assertEqual(sum(len(config.applicable_models(d, 'all')) for d in config.DATASETS[:3]), 22)


if __name__ == '__main__':
    unittest.main()
