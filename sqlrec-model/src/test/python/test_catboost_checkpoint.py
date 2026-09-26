"""CatBoost checkpoint loading without the training runtime dependency."""

import runpy
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from common import filesystem


class CatBoostCheckpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dependency_stubs = {
            "catboost": types.ModuleType("catboost"),
            "numpy": types.ModuleType("numpy"),
            "pandas": types.ModuleType("pandas"),
        }
        with patch.dict(sys.modules, dependency_stubs), patch("logging.basicConfig"):
            cls.module = runpy.run_path(
                str(Path(__file__).resolve().parents[2]
                    / "main/python/gbdt/train_catboost.py")
            )

    def setUp(self):
        self.path = f"memory://{uuid.uuid4().hex}/model.cbm"
        filesystem.write_binary(self.path, b"checkpoint bytes")
        fs, _ = filesystem.url_to_fs(self.path)
        self.addCleanup(fs.rm, self.path)

    def test_local_checkpoint_is_loaded_without_copying(self):
        class FakeModel:
            def load_model(self, path):
                self.path = path
                self.data = Path(path).read_bytes()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.cbm"
            path.write_bytes(b"local checkpoint")
            with (patch.object(self.module["cb"], "CatBoost", FakeModel, create=True),
                  patch.object(self.module["common"], "copy_file") as copy_file):
                model = self.module["_load_base_model"](str(path))

            copy_file.assert_not_called()
            self.assertEqual(model.path, str(path))
            self.assertEqual(model.data, b"local checkpoint")

    def test_remote_checkpoint_is_staged_locally_and_removed(self):
        class FakeModel:
            def load_model(self, path):
                self.path = path
                self.data = Path(path).read_bytes()

        with patch.object(self.module["cb"], "CatBoost", FakeModel, create=True):
            model = self.module["_load_base_model"](self.path)

        self.assertEqual(model.data, b"checkpoint bytes")
        self.assertFalse(Path(model.path).exists())

    def test_temporary_file_is_removed_if_loading_fails(self):
        paths = []

        class FailingModel:
            def load_model(self, path):
                paths.append(path)
                raise RuntimeError("invalid checkpoint")

        with patch.object(self.module["cb"], "CatBoost", FailingModel, create=True):
            with self.assertRaisesRegex(RuntimeError, "invalid checkpoint"):
                self.module["_load_base_model"](self.path)

        self.assertEqual(len(paths), 1)
        self.assertFalse(Path(paths[0]).exists())


if __name__ == "__main__":
    unittest.main()
