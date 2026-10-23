"""Checkpoint structure is retained independently of new model-generation defaults."""

import sys
import json
import math
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
from multi_task import task_contract, validate_predictions
from prediction_contract import export_contract
import torch


class CheckpointConfigTest(unittest.TestCase):
    def test_remote_profiling_is_rejected_before_native_training(self):
        requested = self.config("wide_and_deep")
        requested.model_dir = "jfs://myjfs/model"
        requested.train_config.is_profiling = True
        with self.assertRaisesRegex(ValueError, "local or mounted model_dir"):
            prepare_config(requested, training=True)
        requested.train_config.is_profiling = False
        prepare_config(requested, training=True)

    def test_metrics_can_change_from_legacy_checkpoints_without_changing_structure(self):
        single = text_format.Parse('data_config { label_fields: "label" } '
                'model_config { wide_and_deep {} losses { binary_cross_entropy {} } metrics { auc {} } }',
                pipeline_pb2.EasyRecConfig())
        rocket = config_util.load_pipeline_config("/tests/configs/rocket_launching.config")
        for saved in (single, self.mmoe(), rocket):
            requested = pipeline_pb2.EasyRecConfig()
            requested.CopyFrom(saved)
            saved_owners = saved.model_config.mmoe.task_towers if saved.model_config.WhichOneof("model") == "mmoe" else [saved.model_config]
            for owner in saved_owners:
                del owner.metrics[1:]
                del owner.train_metrics[:]
            requested.train_config.log_step_count_steps = 5
            model = requested.model_config
            owners = model.mmoe.task_towers if model.WhichOneof("model") == "mmoe" else [model]
            for owner in owners:
                del owner.train_metrics[:]
                if owner is model and model.WhichOneof("model") == "wide_and_deep":
                    owner.metrics.add().normalized_entropy.SetInParent()
                    owner.metrics.add().accuracy.SetInParent()
                for metric in owner.metrics:
                    kind = metric.WhichOneof("metric")
                    if kind == "normalized_entropy":
                        continue
                    train_metric = owner.train_metrics.add(decay_step=10, decay_rate=.8)
                    getattr(train_metric, kind).CopyFrom(getattr(metric, kind))
            with self.subTest(architecture=model.WhichOneof("model")):
                effective = prepare_config(requested, saved, training=True, check_checkpoint_structure=True)
                self.assertEqual(effective.model_config, requested.model_config)
                exported = prepare_config(saved, effective, training=False, check_checkpoint_structure=True)
                self.assertEqual(exported.model_config, effective.model_config)
                disabled = prepare_config(saved, effective, training=True, check_checkpoint_structure=True)
                self.assertEqual(disabled.model_config, saved.model_config)
                self.assertEqual(effective.feature_configs, saved.feature_configs)

    def test_training_metrics_validate_decay_and_task_type(self):
        for change in ("type", "duplicate", "zero_step", "unaligned_step", "rate", "log_step"):
            requested = self.mmoe()
            for tower in requested.model_config.mmoe.task_towers:
                del tower.train_metrics[:]
            metric = requested.model_config.mmoe.task_towers[1].train_metrics.add()
            metric.mean_squared_error.SetInParent()
            if change == "type":
                metric.auc.SetInParent()
            elif change == "duplicate":
                requested.model_config.mmoe.task_towers[1].train_metrics.add().CopyFrom(metric)
            elif change == "zero_step":
                metric.decay_step = 0
            elif change == "unaligned_step":
                metric.decay_step = 150
            elif change == "rate":
                metric.decay_rate = float("nan")
            else:
                requested.train_config.log_step_count_steps = 0
            with self.subTest(change=change), self.assertRaises(ValueError):
                prepare_config(requested, training=True)

    def test_pool_checkpoint_preserves_settings_but_refreshes_training_source(self):
        requested = text_format.Parse('data_config { label_fields: "label" } '
                'model_config { dssm {} losses { softmax_cross_entropy {} } }', pipeline_pb2.EasyRecConfig())
        saved = pipeline_pb2.EasyRecConfig()
        saved.CopyFrom(requested)
        saved.data_config.pool_negative_sampler.input_path = "/old/*.parquet"
        saved.data_config.pool_negative_sampler.num_sample = 256
        saved.data_config.pool_negative_sampler.pool_size = 4096
        saved.data_config.pool_negative_sampler.item_id_field = "id"
        saved.data_config.pool_negative_sampler.attr_fields.append("id")
        requested.train_input_path = "/new/*.parquet"
        self.assertEqual(prepare_config(requested, saved).data_config.pool_negative_sampler.input_path,
                         "/old/*.parquet")
        effective = prepare_config(requested, saved, training=True)
        self.assertEqual(effective.data_config.pool_negative_sampler.input_path, "/new/*.parquet")
        self.assertEqual(effective.data_config.pool_negative_sampler.pool_size, 4096)
        for key, value in (("negative_sampling", "in_batch"), ("negative_sample_count", "512"),
                           ("negative_pool_size", "2048"), ("item_id_column", "other")):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "override"):
                prepare_config(requested, saved, structure_options={key: value})
        prepare_config(requested, saved, structure_options={"negative_sampling": "pool",
                       "negative_sample_count": "256", "negative_pool_size": "4096", "item_id_column": "id"})

    def test_dssm_unsupported_objectives_are_rejected_before_checkpoint_merge(self):
        def recall():
            return text_format.Parse(
                'data_config { label_fields: "label" } '
                'model_config { dssm {} losses { softmax_cross_entropy {} } }',
                pipeline_pb2.EasyRecConfig())

        for objective in ("binary_cross_entropy", "l2_loss"):
            unsupported = recall()
            getattr(unsupported.model_config.losses[0], objective).SetInParent()
            for requested, saved in ((unsupported, None), (unsupported, recall()), (recall(), unsupported)):
                with self.subTest(objective=objective, saved=saved), self.assertRaisesRegex(
                        ValueError, "DSSM requires exactly one softmax_cross_entropy"):
                    prepare_config(requested, saved)
        saved, requested = recall(), recall()
        requested.data_config.batch_size = 16
        self.assertEqual(prepare_config(requested, saved).data_config.batch_size, 16)
        for count in (0, 1, 2):
            malformed = recall()
            del malformed.model_config.losses[:]
            for _ in range(count):
                loss = malformed.model_config.losses.add()
                if count == 2:
                    loss.softmax_cross_entropy.SetInParent()
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, "DSSM requires exactly one"):
                prepare_config(malformed)

    def test_cli_rejects_unsupported_dssm_checkpoint_before_training_or_export(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "checkpoint"
            source.mkdir()
            requested = text_format.Parse(
                'data_config { label_fields: "label" } '
                'model_config { dssm {} losses { softmax_cross_entropy {} } }',
                pipeline_pb2.EasyRecConfig())
            saved = pipeline_pb2.EasyRecConfig()
            saved.CopyFrom(requested)
            saved.model_config.losses[0].binary_cross_entropy.SetInParent()
            config_util.save_message(saved, str(source / "pipeline.config"))
            requested.model_dir = str(source)
            requested.train_config.fine_tune_checkpoint = str(source)
            path = root / "requested.config"
            config_util.save_message(requested, str(path))
            for mode in ("train", "export"):
                argv = ["run.py", "--mode", mode, "--pipeline_config_path", str(path),
                        "--export_dir", str(root / "export")]
                with (self.subTest(mode=mode), patch.object(sys, "argv", argv),
                      patch("tzrec.main.train_and_evaluate") as train,
                      patch("tzrec.main.export") as export):
                    with self.assertRaisesRegex(ValueError, "DSSM requires exactly one softmax_cross_entropy"):
                        main()
                    train.assert_not_called()
                    export.assert_not_called()
                self.assertFalse((root / "export").exists())
                self.assertFalse(list(root.glob("export.staging-*")))

    def test_training_entry_delegates_sample_loading_to_native_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for architecture in ("wide_and_deep", "deepfm", "dssm", "mmoe", "rocket_launching"):
                if architecture == "mmoe":
                    config = self.mmoe()
                elif architecture == "rocket_launching":
                    config = config_util.load_pipeline_config("/tests/configs/rocket_launching.config")
                elif architecture == "dssm":
                    config = text_format.Parse(
                        'data_config { label_fields: "label" } model_config { dssm {} losses { softmax_cross_entropy {} } }',
                        pipeline_pb2.EasyRecConfig())
                else:
                    config = self.config(architecture)
                # These paths do not exist: only the native trainer should read samples.
                config.train_input_path = str(root / "train.parquet")
                config.eval_input_path = str(root / "eval.parquet")
                config.model_dir = str(root / "model")
                path = root / "pipeline.config"
                config_util.save_message(config, str(path))
                for world_size, rank in ((1, 0), (2, 0), (2, 1)):
                    snapshots = []
                    with (self.subTest(architecture=architecture, world_size=world_size, rank=rank),
                          patch.dict(os.environ, {"WORLD_SIZE": str(world_size), "RANK": str(rank)}),
                          patch.object(sys, "argv", ["run.py", "--mode", "train", "--pipeline_config_path", str(path)]),
                          patch("torch.distributed.rendezvous", side_effect=AssertionError("unexpected pre-training rendezvous")),
                          patch("tzrec.main.train_and_evaluate", side_effect=lambda path, **kwargs:
                                snapshots.append(config_util.load_pipeline_config(path))) as train):
                        main()
                        train.assert_called_once()
                        self.assertEqual(snapshots[0].train_input_path, config.train_input_path)
                        self.assertEqual(snapshots[0].eval_input_path, config.eval_input_path)

    def test_explicit_single_task_structure_changes_cannot_be_silently_ignored(self):
        saved = self.config("wide_and_deep")
        requested = self.config("wide_and_deep")
        requested.data_config.batch_size = 16
        self.assertEqual(prepare_config(requested, saved, True).data_config.batch_size, 16)
        requested.feature_configs[0].id_feature.embedding_dim = 32
        with self.assertRaisesRegex(ValueError, "Explicit network and feature overrides"):
            prepare_config(requested, saved, True)
        requested = self.config("deepfm")
        with self.assertRaisesRegex(ValueError, "Checkpoint architecture"):
            prepare_config(requested, saved, True)

    def test_selective_overrides_preserve_checkpoint_defaults_with_real_protobufs(self):
        saved = self.config("deepfm")
        saved.feature_configs[0].id_feature.embedding_dim = 8
        requested = self.config("deepfm")
        effective = prepare_config(requested, saved, structure_options={"hidden_units": "8,4"})
        self.assertEqual(effective.feature_configs, saved.feature_configs)
        self.assertEqual(effective.model_config, saved.model_config)
        with self.assertRaisesRegex(ValueError, "hidden_units"):
            prepare_config(requested, saved, structure_options={"hidden_units": "16,8"})
        with self.assertRaisesRegex(ValueError, "embedding_dim"):
            prepare_config(requested, saved, structure_options={"column.id.embedding_dim": "16"})

    def test_cli_applies_selective_checkpoint_validation_before_native_training(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            saved = self.config("deepfm")
            saved.feature_configs[0].id_feature.embedding_dim = 8
            config_util.save_message(saved, str(source / "pipeline.config"))
            requested = self.config("deepfm")
            requested.train_config.fine_tune_checkpoint = str(source)
            path = root / "requested.config"
            config_util.save_message(requested, str(path))
            argv = ["run.py", "--mode", "train", "--pipeline_config_path", str(path),
                    "--checkpoint_structure_options", json.dumps({"options": {"hidden_units": "8,4"}})]
            snapshots = []
            with (patch.object(sys, "argv", argv),
                  patch("tzrec.main.train_and_evaluate", side_effect=lambda path, **kwargs:
                        snapshots.append(config_util.load_pipeline_config(path)))):
                main()
            self.assertEqual(snapshots[0].model_config, saved.model_config)
            self.assertEqual(snapshots[0].feature_configs, saved.feature_configs)

    def mmoe(self, mixed=True):
        name = "mmoe_mixed" if mixed else "mmoe"
        return config_util.load_pipeline_config(f"/tests/configs/{name}.config")

    def test_checkpoint_accepts_equivalent_raw_defaults_and_normalizers(self):
        for architecture in ("wide_and_deep", "mmoe"):
            saved = self.mmoe() if architecture == "mmoe" else self.config(architecture)
            if architecture == "mmoe":
                raw = saved.feature_configs[1].raw_feature
            else:
                raw = saved.feature_configs.add().raw_feature
                raw.feature_name, raw.expression = "price", "item:price"
                saved.model_config.feature_groups[1].feature_names.append("price")
            raw.default_value = "0"
            raw.normalizer = "method=zscore,mean=0,standard_deviation=1"
            requested = pipeline_pb2.EasyRecConfig()
            requested.CopyFrom(saved)
            index = 1
            requested.feature_configs[index].raw_feature.default_value = "0.0"
            requested.feature_configs[index].raw_feature.normalizer = "standard_deviation=1.0,mean=0e0,method=zscore"
            options = {"column.price.default_value": "0.0",
                       "column.price.normalizer": requested.feature_configs[index].raw_feature.normalizer}
            with self.subTest(architecture=architecture):
                effective = prepare_config(requested, saved, structure_options=options)
                self.assertEqual(effective.feature_configs, saved.feature_configs)
                effective = prepare_config(requested, saved, check_checkpoint_structure=True)
                self.assertEqual(effective.feature_configs, saved.feature_configs)
                requested.feature_configs[index].raw_feature.default_value = "1"
                with self.assertRaises(ValueError):
                    prepare_config(requested, saved, structure_options={"column.price.default_value": "1"})
                with self.assertRaises(ValueError):
                    prepare_config(requested, saved, check_checkpoint_structure=True)

    def test_java_generated_mmoe_configs_parse_and_have_the_declared_outputs(self):
        for name in ("mmoe", "mmoe_mixed", "mmoe_dense"):
            config = config_util.load_pipeline_config(f"/tests/configs/{name}.config")
            effective = prepare_config(config)
            contract = task_contract(effective)
            self.assertEqual([task["label"] for task in contract["tasks"]], list(config.data_config.label_fields))
            self.assertEqual([field["name"] for field in contract["output_fields"]],
                             ["probs_label", "y_watch_time" if name == "mmoe_mixed" else "probs_label_like"])
            features = {getattr(feature, feature.WhichOneof("feature")).feature_name for feature in config.feature_configs}
            self.assertFalse(features & set(config.data_config.label_fields))

    def test_mmoe_checkpoint_retains_structure_and_runtime_changes(self):
        saved = self.mmoe()
        requested = self.mmoe()
        requested.data_config.batch_size = 16
        requested.model_dir = "/new/model"
        requested.eval_input_path = "/new/eval.parquet"
        requested.train_config.dense_optimizer.adam_optimizer.lr = .02
        effective = prepare_config(requested, saved)
        self.assertEqual(effective.model_config, saved.model_config)
        self.assertEqual(effective.data_config.batch_size, 16)
        self.assertEqual(effective.eval_input_path, "/new/eval.parquet")
        for change in ("order", "loss", "weight", "network", "features"):
            requested = self.mmoe()
            if change == "order":
                labels = list(requested.data_config.label_fields)
                del requested.data_config.label_fields[:]
                requested.data_config.label_fields.extend(reversed(labels))
            elif change == "loss":
                requested.model_config.mmoe.task_towers[0].losses[0].l2_loss.SetInParent()
            elif change == "weight":
                requested.model_config.mmoe.task_towers[0].weight = 2
            elif change == "network":
                requested.model_config.mmoe.num_expert = 4
            else:
                requested.feature_configs[0].id_feature.embedding_dim = 16
            with self.subTest(change=change), self.assertRaises(ValueError):
                prepare_config(requested, saved)

    def test_mmoe_rejects_invalid_tasks_and_label_features(self):
        for change in ("duplicate", "name", "metric", "weight", "label_feature", "mask"):
            requested = self.mmoe()
            tower = requested.model_config.mmoe.task_towers[0]
            if change == "duplicate":
                requested.data_config.label_fields[1] = requested.data_config.label_fields[0]
            elif change == "name":
                tower.tower_name = "renamed"
            elif change == "metric":
                tower.metrics[0].mean_squared_error.SetInParent()
            elif change == "weight":
                tower.weight = float("nan")
            elif change == "label_feature":
                requested.feature_configs[0].id_feature.expression = "item:label"
            else:
                tower.task_space_indicator_label = "label"
            with self.subTest(change=change), self.assertRaises(ValueError):
                prepare_config(requested)

    def test_export_predictions_require_all_tasks_and_the_batch_shape(self):
        contract = task_contract(self.mmoe())
        valid = {"probs_label": torch.tensor([.2, .3]), "y_watch_time": torch.tensor([1., 2.])}
        validate_predictions(valid, contract, 2)
        for invalid in ({"probs_label": valid["probs_label"]},
                        {**valid, "y_watch_time": torch.ones(2, 1)},
                        {**valid, "y_watch_time": torch.tensor([float("nan"), 2.])},
                        {**valid, "y_watch_time": torch.ones(2, dtype=torch.int64)}):
            with self.assertRaisesRegex(ValueError, "Invalid MMoE output"):
                validate_predictions(invalid, contract, 2)

    def test_native_mmoe_losses_apply_task_weights_and_backpropagate(self):
        from tzrec.datasets.utils import Batch
        from tzrec.features.feature import create_features
        from tzrec.models.mmoe import MMoE
        import torch.nn.functional as functional

        config = self.mmoe()
        config.model_config.mmoe.task_towers[0].weight = 2.0
        model = MMoE(config.model_config,
                     create_features(list(config.feature_configs), config.data_config.fg_mode),
                     list(config.data_config.label_fields))
        model.init_loss()
        logits = torch.tensor([-.5, .5], requires_grad=True)
        regression = torch.tensor([2., 4.], requires_grad=True)
        labels = torch.tensor([0, 1])
        batch = Batch(labels={"label": labels, "watch_time": torch.tensor([1., 7.])})
        losses = model.loss({"logits_label": logits, "y_watch_time": regression}, batch)
        self.assertEqual(set(losses), {"binary_cross_entropy_label", "l2_loss_watch_time"})
        torch.testing.assert_close(losses["binary_cross_entropy_label"],
                                   2 * functional.binary_cross_entropy_with_logits(logits, labels.float()))
        torch.testing.assert_close(losses["l2_loss_watch_time"], torch.tensor(.5))
        sum(losses.values()).backward()
        torch.testing.assert_close(logits.grad, logits.detach().sigmoid() - labels.float())
        torch.testing.assert_close(regression.grad, torch.tensor([.1, -.3]))

    def test_default_binary_and_regression_metrics_use_the_correct_predictions(self):
        from tzrec.datasets.utils import Batch
        from tzrec.features.feature import create_features
        from tzrec.models.mmoe import MMoE
        import torch.nn.functional as functional

        config = self.mmoe()
        for tower in config.model_config.mmoe.task_towers:
            for metric in tower.train_metrics:
                metric.decay_step = 1
        model = MMoE(config.model_config,
                     create_features(list(config.feature_configs), config.data_config.fg_mode),
                     list(config.data_config.label_fields))
        model.init_loss()
        model.init_metric()
        probabilities = torch.tensor([.1, .4, .35, .8])
        binary = torch.tensor([0, 0, 1, 1])
        batch = Batch(labels={"label": binary, "watch_time": torch.tensor([1., 7., 1., 6.])})
        predictions = {"probs_label": probabilities, "logits_label": probabilities.logit(),
                       "y_watch_time": torch.tensor([2., 4., 1., 8.])}
        losses = model.loss(predictions, batch)
        model.update_metric(predictions, batch, losses)
        model.update_train_metric(predictions, batch)
        evaluated, trained = model.compute_metric(), model.compute_train_metric()
        expected = {"auc_label": .75, "accuracy_label": .75,
                    "mean_squared_error_watch_time": 3.5, "mean_absolute_error_watch_time": 1.5}
        for name, value in expected.items():
            with self.subTest(metric=name):
                self.assertAlmostEqual(evaluated[name].item(), value, places=5)
                self.assertAlmostEqual(trained[name].item(), value, places=5)
        expected_ne = functional.binary_cross_entropy(probabilities, binary.float()).item() / math.log(2)
        self.assertAlmostEqual(evaluated["normalized_entropy_label"].item(), expected_ne, places=5)
        self.assertNotIn("normalized_entropy_label", trained)

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

    def test_prediction_validation_failure_does_not_publish_and_can_retry(self):
        for config in (self.mmoe(), config_util.load_pipeline_config("/tests/configs/rocket_launching.config")):
            with self.subTest(architecture=config.model_config.WhichOneof("model")):
                self.check_prediction_publication_failure(config)

    def check_prediction_publication_failure(self, config):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "checkpoint"
            source.mkdir()
            config.model_dir = str(source)
            config_util.save_message(config, str(source / "pipeline.config"))
            path = root / "requested.config"
            config_util.save_message(config, str(path))
            target = root / "export"
            stages = []

            def fake_export(config_path, export_dir):
                stage = Path(export_dir)
                stages.append(stage)
                stage.mkdir()
                (stage / "scripted_model.pt").write_bytes(b"fixture")
                (stage / "fg.json").write_text(json.dumps({"features": []}))
                (stage / "pipeline.config").write_text(Path(config_path).read_text())

            argv = ["run.py", "--mode", "export", "--pipeline_config_path", str(path), "--export_dir", str(target)]
            with (patch.object(sys, "argv", argv), patch.dict(os.environ, {"WORLD_SIZE": "1", "RANK": "0"}),
                  patch("tzrec.main.export", side_effect=fake_export),
                  patch("run.validate_scripted_export", side_effect=[ValueError("Invalid prediction output"), None]) as validate):
                with self.assertRaisesRegex(ValueError, "Invalid prediction output"):
                    main()
                self.assertFalse(target.exists())
                self.assertFalse(any(root.glob("export.staging-*")))
                main()
                self.assertEqual(validate.call_count, 2)
            self.assertNotEqual(stages[0], stages[1])
            self.assertFalse(any(root.glob("export.staging-*")))
            self.assertTrue((target / "_SUCCESS").is_file())
            metadata = json.loads((target / "model_meta.json").read_text())
            for key, value in export_contract(config).items():
                self.assertEqual(metadata[key], value)

    def config(self, architecture):
        return text_format.Parse('''
          data_config { label_fields: "label" fg_mode: FG_NORMAL batch_size: 8 num_workers: 0 }
          feature_configs { id_feature { feature_name: "id" expression: "item:id" num_buckets: 100 embedding_dim: 16 } }
          model_config {
            feature_groups { group_name: "wide" group_type: WIDE feature_names: "id" }
            feature_groups { group_name: "deep" group_type: DEEP feature_names: "id" }
        ''' + architecture + ' { deep { hidden_units: [8,4] } } }', pipeline_pb2.EasyRecConfig())

    def test_same_architecture_retains_saved_features_groups_and_runtime_overrides(self):
        requested = self.config("deepfm")
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
        self.assertEqual(requested.model_config.WhichOneof("model"), "deepfm")

    def test_label_changes_and_label_features_require_retraining(self):
        requested = self.config("deepfm")
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
