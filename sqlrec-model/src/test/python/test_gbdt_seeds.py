"""Explicit benchmark seeds reach each native library."""
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch
from common import filesystem  # Load Arrow once, outside temporary dependency stubs.


class GbdtSeedTest(unittest.TestCase):
    def test_seed_mapping_to_each_library(self):
        for framework, builder, key in [('lightgbm', '_build_lgb_params', 'seed'),
                                        ('xgboost', '_build_xgb_params', 'seed'),
                                        ('catboost', '_build_cb_params', 'random_seed')]:
            with self.subTest(framework=framework), patch.dict(sys.modules, {name: types.ModuleType(name) for name in (framework, 'numpy', 'pandas')}):
                module = runpy.run_path(str(Path(__file__).resolve().parents[2] / f'main/python/gbdt/train_{framework}.py'))
                self.assertEqual(module[builder]({'random_seed': 789})[key], 789)
                self.assertEqual(module[builder]({})[key], 0)


if __name__ == '__main__':
    unittest.main()
