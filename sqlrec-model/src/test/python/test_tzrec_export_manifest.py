"""Complete-export markers and checksums are enforced without Torch dependencies."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[2] / "main/python/tzrec/validate_export.py"
spec = importlib.util.spec_from_file_location("validate_export", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ExportManifestTest(unittest.TestCase):
    def test_legacy_exports_check_labels_from_the_pipeline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in module.REQUIRED_FILES:
                (root / name).write_text("fixture")
            (root / "fg.json").write_text(json.dumps({"features": [{"feature_name": "alias", "expression": "item:label"}]}))
            with self.assertRaisesRegex(ValueError, "label features"):
                module.validate_export(temporary, ["label"])

    def test_legacy_hash_exports_reject_an_incompatible_hash_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in module.REQUIRED_FILES:
                (root / name).write_text("fixture")
            (root / "fg.json").write_text(json.dumps({"features": [{"feature_name": "id", "expression": "item:id", "hash_bucket_size": 100}]}))
            with patch.dict("os.environ", {"USE_FARM_HASH_TO_BUCKETIZE": "false"}):
                with self.assertRaisesRegex(ValueError, "Hash features require"):
                    module.validate_export(temporary)

    def test_legacy_complete_and_corrupt_exports(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in module.REQUIRED_FILES:
                (root / name).write_text("fixture")
            (root / "fg.json").write_text(json.dumps({"features": [{"feature_name": "id", "expression": "item:id"}]}))
            module.validate_export(temporary)
            metadata = {"format_version": 1, "sha256": {name: module.digest(root / name) for name in module.REQUIRED_FILES}, "labels": ["label"]}
            (root / "model_meta.json").write_text(json.dumps(metadata))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                module.validate_export(temporary)
            (root / "_SUCCESS").touch()
            module.validate_export(temporary)
            module.validate_export(temporary, ["label"])
            with self.assertRaisesRegex(ValueError, "label_fields"):
                module.validate_export(temporary, ["different_label"])
            (root / "scripted_model.pt").write_text("corrupt")
            with self.assertRaisesRegex(ValueError, "checksum"):
                module.validate_export(temporary)

    def test_declared_labels_cannot_be_features(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in module.REQUIRED_FILES:
                (root / name).write_text("fixture")
            (root / "fg.json").write_text(json.dumps({"features": [{"feature_name": "alias", "expression": "item:label"}]}))
            metadata = {"format_version": 1, "sha256": {name: module.digest(root / name) for name in module.REQUIRED_FILES}, "labels": ["label"]}
            (root / "model_meta.json").write_text(json.dumps(metadata))
            (root / "_SUCCESS").touch()
            with self.assertRaisesRegex(ValueError, "label features"):
                module.validate_export(temporary)
