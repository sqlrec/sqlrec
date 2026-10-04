"""Checkpoint structure is retained independently of new model-generation defaults."""

import sys
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, "/app")
from google.protobuf import text_format
from tzrec.protos import pipeline_pb2
from run import prepare_config, main
from tzrec.utils import config_util


class CheckpointConfigTest(unittest.TestCase):
    def test_export_retry_uses_a_fresh_staging_directory_and_cleans_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "checkpoint"
            source.mkdir()
            requested = self.config("wide_and_deep")
            requested.model_dir = str(source)
            config_util.save_message(requested, str(source / "pipeline.config"))
            path = root / "requested.config"
            config_util.save_message(requested, str(path))
            target = root / "export"
            stages = []

            def fake_export(config_path, export_dir):
                stage = Path(export_dir)
                stages.append(stage)
                stage.mkdir()
                if len(stages) == 1:
                    raise RuntimeError("Injected export failure")
                (stage / "scripted_model.pt").write_bytes(b"fixture")
                (stage / "fg.json").write_text(json.dumps({"features": [{"feature_name": "id", "expression": "item:id"}]}))
                (stage / "pipeline.config").write_text(Path(config_path).read_text())

            argv = ["run.py", "--mode", "export", "--pipeline_config_path", str(path), "--export_dir", str(target)]
            with (patch.object(sys, "argv", argv), patch.dict(os.environ, {"JOB_NAME": "same-job", "WORLD_SIZE": "1", "RANK": "0"}),
                  patch("tzrec.main.export", side_effect=fake_export)):
                with self.assertRaisesRegex(RuntimeError, "Injected export failure"):
                    main()
                self.assertFalse(target.exists())
                main()
            self.assertNotEqual(stages[0], stages[1])
            self.assertFalse(any(root.glob("export.staging-*")))
            self.assertTrue((target / "_SUCCESS").is_file())
            self.assertTrue((target / "model_meta.json").is_file())

    def test_export_rejects_multiple_processes_before_loading_config(self):
        with (patch.object(sys, "argv", ["run.py", "--mode", "export", "--pipeline_config_path", "/missing"]),
              patch.dict(os.environ, {"WORLD_SIZE": "2"}), patch("run.config_util.load_pipeline_config") as load):
            with self.assertRaisesRegex(ValueError, "single process"):
                main()
            load.assert_not_called()

    def config(self, architecture):
        return text_format.Parse('''
          data_config { label_fields: "label" fg_mode: FG_NORMAL batch_size: 8 num_workers: 0 }
          feature_configs { id_feature { feature_name: "id" expression: "item:id" num_buckets: 100 embedding_dim: 16 } }
          model_config {
            feature_groups { group_name: "wide" group_type: WIDE feature_names: "id" }
            feature_groups { group_name: "deep" group_type: DEEP feature_names: "id" }
        ''' + architecture + ' { deep { hidden_units: [8,4] } } }', pipeline_pb2.EasyRecConfig())

    def test_legacy_deepfm_retains_saved_features_groups_and_architecture(self):
        requested = self.config("wide_and_deep")
        saved = self.config("deepfm")
        requested.data_config.batch_size = 32
        requested.model_dir = "/new/checkpoint"
        requested.train_config.num_epochs = 7
        saved.feature_configs[0].id_feature.embedding_dim = 8
        effective = prepare_config(requested, saved)
        self.assertEqual(effective.model_config.WhichOneof("model"), "deepfm")
        self.assertEqual(effective.model_config, saved.model_config)
        self.assertEqual(effective.feature_configs, saved.feature_configs)
        self.assertEqual(effective.data_config.batch_size, 32)
        self.assertEqual(effective.model_dir, "/new/checkpoint")
        self.assertEqual(effective.train_config.num_epochs, 7)
        self.assertEqual(requested.model_config.WhichOneof("model"), "wide_and_deep")

    def test_label_changes_and_label_features_require_retraining(self):
        requested = self.config("wide_and_deep")
        saved = self.config("deepfm")
        saved.data_config.label_fields[0] = "other"
        with self.assertRaisesRegex(ValueError, "label_fields"):
            prepare_config(requested, saved)
        saved.data_config.label_fields[0] = "label"
        saved.feature_configs[0].id_feature.expression = "item:label"
        with self.assertRaisesRegex(ValueError, "label features"):
            prepare_config(requested, saved)


if __name__ == "__main__":
    unittest.main()
