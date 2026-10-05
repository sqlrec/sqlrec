"""Checkpoint comparisons use protobuf presence, equality and copy semantics."""

import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def config_type():
    # Small wire-compatible fixture for the fields read by prepare_config.
    file = descriptor_pb2.FileDescriptorProto(name="checkpoint_fixture.proto", syntax="proto2")
    def message(name, fields, oneof=None):
        result = file.message_type.add(name=name)
        if oneof:
            result.oneof_decl.add(name=oneof)
        for number, (field_name, field_type, repeated) in enumerate(fields, 1):
            field = result.field.add(name=field_name, number=number, label=3 if repeated else 1)
            if isinstance(field_type, str):
                field.type, field.type_name = 11, field_type
            else:
                field.type = field_type
            if oneof:
                field.oneof_index = 0
        return result
    message("Data", [("label_fields", 9, True), ("batch_size", 5, False), ("num_workers", 5, False)])
    message("Empty", [])
    message("Loss", [("softmax_cross_entropy", ".Empty", False),
                     ("binary_cross_entropy", ".Empty", False), ("l2_loss", ".Empty", False)], "loss")
    message("Mlp", [("hidden_units", 5, True)])
    message("Network", [("deep", ".Mlp", False)])
    message("Tower", [("input", 9, False), ("mlp", ".Mlp", False)])
    message("Dssm", [("user_tower", ".Tower", False), ("item_tower", ".Tower", False),
                     ("output_dim", 5, False)])
    message("Group", [("group_name", 9, False), ("feature_names", 9, True)])
    model = message("Model", [("wide_and_deep", ".Network", False), ("deepfm", ".Network", False),
                              ("dssm", ".Dssm", False)], "model")
    model.field.add(name="feature_groups", number=4, label=3, type=11, type_name=".Group")
    model.field.add(name="mmoe", number=5, label=1, type=11, type_name=".Network", oneof_index=0)
    model.field.add(name="losses", number=6, label=3, type=11, type_name=".Loss")
    message("Id", [("feature_name", 9, False), ("expression", 9, False),
                   ("embedding_dim", 5, False), ("num_buckets", 5, False), ("hash_bucket_size", 5, False),
                   ("default_value", 9, False), ("separator", 9, False)])
    message("AutoDis", [("num_channels", 5, False)])
    raw = message("Raw", [("feature_name", 9, False), ("expression", 9, False), ("embedding_dim", 5, False),
                          ("value_dim", 5, False), ("default_value", 9, False), ("separator", 9, False),
                          ("normalizer", 9, False), ("boundaries", 2, True)])
    raw.oneof_decl.add(name="dense_emb")
    for field in raw.field:
        if field.name == "value_dim":
            field.default_value = "1"
        elif field.name == "default_value":
            field.default_value = "0"
        elif field.name == "separator":
            field.default_value = "\035"
    raw.field.add(name="mlp", number=9, label=1, type=11, type_name=".Mlp", oneof_index=0)
    raw.field.add(name="autodis", number=10, label=1, type=11, type_name=".AutoDis", oneof_index=0)
    message("Feature", [("id_feature", ".Id", False), ("raw_feature", ".Raw", False)], "feature")
    message("Pipeline", [("data_config", ".Data", False), ("model_config", ".Model", False),
                         ("feature_configs", ".Feature", True)])
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("Pipeline"))


class CheckpointStructureTest(unittest.TestCase):
    def setUp(self):
        self.Config = config_type()
        tasks = ModuleType("multi_task")
        tasks.task_contract = lambda config: None
        path = Path(__file__).resolve().parents[2] / "main/python/tzrec/pipeline_config.py"
        sys.path.insert(0, str(path.parent))
        spec = importlib.util.spec_from_file_location("checkpoint_config_test", path)
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"multi_task": tasks}):
            spec.loader.exec_module(self.module)

    def config(self, architecture="wide_and_deep"):
        config = self.Config()
        config.data_config.label_fields.append("label")
        config.data_config.batch_size = 8
        if architecture == "dssm":
            config.model_config.dssm.user_tower.input = "user"
            config.model_config.dssm.item_tower.input = "item"
            config.model_config.losses.add().softmax_cross_entropy.SetInParent()
        else:
            getattr(config.model_config, architecture).deep.hidden_units.extend([8, 4])
        feature = config.feature_configs.add().id_feature
        feature.feature_name, feature.expression = "id", "item:id"
        feature.embedding_dim, feature.num_buckets = 16, 100
        return config

    def test_explicit_feature_and_network_changes_fail(self):
        saved = self.config()
        for change in ("network", "feature", "buckets", "architecture"):
            requested = self.config()
            if change == "network":
                requested.model_config.wide_and_deep.deep.hidden_units.append(2)
            elif change == "feature":
                requested.feature_configs[0].id_feature.embedding_dim = 32
            elif change == "buckets":
                requested.feature_configs[0].id_feature.num_buckets = 200
            else:
                requested = self.config("deepfm")
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "Explicit network and feature overrides"):
                self.module.prepare_config(requested, saved, True)

    def test_matching_structure_and_runtime_changes_are_allowed(self):
        saved, requested = self.config(), self.config()
        requested.data_config.batch_size = 16
        effective = self.module.prepare_config(requested, saved, True)
        self.assertEqual(effective.model_config, saved.model_config)
        self.assertEqual(effective.data_config.batch_size, 16)
        self.module.prepare_config(requested, None, True)

    def test_strict_checks_accept_only_equivalent_raw_feature_format_changes(self):
        for architecture in ("wide_and_deep", "deepfm"):
            saved = self.config(architecture)
            raw = saved.feature_configs.add().raw_feature
            raw.feature_name, raw.expression, raw.default_value = "price", "item:price", "0"
            raw.normalizer = "method=zscore,mean=0,standard_deviation=1"
            requested = self.Config()
            requested.CopyFrom(saved)
            requested.data_config.batch_size = 16
            requested.feature_configs[1].raw_feature.default_value = "0.0"
            requested.feature_configs[1].raw_feature.normalizer = "standard_deviation=1.0,mean=0e0,method=zscore"
            with self.subTest(architecture=architecture):
                effective = self.module.prepare_config(requested, saved, check_checkpoint_structure=True)
                self.assertEqual(effective.feature_configs, saved.feature_configs)
                self.assertEqual(effective.model_config, saved.model_config)
                self.assertEqual(effective.data_config.batch_size, 16)
                self.assertEqual(requested.feature_configs[1].raw_feature.default_value, "0.0")
            for field, value in (("default_value", "1"), ("value_dim", 2), ("separator", "|"),
                                 ("normalizer", "method=zscore,mean=1,standard_deviation=1")):
                changed = self.Config()
                changed.CopyFrom(requested)
                setattr(changed.feature_configs[1].raw_feature, field, value)
                with self.subTest(architecture=architecture, field=field), self.assertRaisesRegex(
                        ValueError, "Explicit network and feature overrides"):
                    self.module.prepare_config(changed, saved, check_checkpoint_structure=True)

    def test_legacy_checkpoint_defaults_are_preserved_without_explicit_overrides(self):
        saved, requested = self.config("deepfm"), self.config()
        saved.feature_configs[0].id_feature.embedding_dim = 8
        effective = self.module.prepare_config(requested, saved)
        self.assertEqual(effective.model_config.WhichOneof("model"), "deepfm")
        self.assertEqual(effective.feature_configs[0].id_feature.embedding_dim, 8)

    def test_repeating_hidden_units_does_not_compare_unrelated_embedding_defaults(self):
        for architecture in ("wide_and_deep", "deepfm"):
            saved, requested = self.config(architecture), self.config()
            saved.feature_configs[0].id_feature.embedding_dim = 8
            effective = self.module.prepare_config(requested, saved, structure_options={"hidden_units": "8, 4"})
            self.assertEqual(effective.feature_configs[0].id_feature.embedding_dim, 8)
            self.assertEqual(effective.model_config, saved.model_config)
            with self.assertRaisesRegex(ValueError, "hidden_units"):
                self.module.prepare_config(requested, saved, structure_options={"hidden_units": "16,8"})

    def test_feature_overrides_compare_only_the_named_option(self):
        saved, requested = self.config(), self.config()
        saved.feature_configs[0].id_feature.embedding_dim = 8
        self.module.prepare_config(requested, saved, structure_options={"column.id.bucket_size": "100"})
        for options in ({"column.id.bucket_size": "200"}, {"column.id.embedding_dim": "16"},
                        {"column.missing.embedding_dim": "16"}):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, "Explicit structure override"):
                self.module.prepare_config(requested, saved, structure_options=options)

    def test_global_defaults_respect_per_column_precedence(self):
        saved, requested = self.config(), self.config()
        saved.feature_configs[0].id_feature.embedding_dim = 8
        options = {"embedding_dim": "16", "num_buckets": "200"}
        masks = {"embedding_dim": ["id"], "num_buckets": ["id"]}
        self.module.prepare_config(requested, saved, structure_options=options, structure_masks=masks)
        with self.assertRaisesRegex(ValueError, "embedding_dim"):
            self.module.prepare_config(requested, saved, structure_options=options)
        with self.assertRaisesRegex(ValueError, "num_buckets"):
            self.module.prepare_config(requested, saved, structure_options={"num_buckets": "200"})

    def test_raw_feature_options_preserve_unrelated_network_and_feature_defaults(self):
        saved, requested = self.config(), self.config()
        raw = saved.feature_configs.add().raw_feature
        raw.feature_name, raw.expression = "vector", "item:vector"
        raw.value_dim, raw.embedding_dim, raw.default_value = 2, 8, "0\0350"
        raw.separator, raw.normalizer = "\035", "method=zscore,mean=0,standard_deviation=1"
        raw.mlp.SetInParent()
        options = {"column.vector.value_dim": "2", "column.vector.embedding_dim": "8",
                   "column.vector.default_value": "0\0350", "column.vector.separator": "\035",
                   "column.vector.normalizer": raw.normalizer, "column.vector.embedding": "mlp"}
        effective = self.module.prepare_config(requested, saved, structure_options=options)
        self.assertEqual(effective.feature_configs, saved.feature_configs)
        for key, value in (("value_dim", "3"), ("embedding_dim", "16"), ("embedding", "none"),
                           ("normalizer", "method=log10"), ("separator", "|"), ("default_value", "1\0351")):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                self.module.prepare_config(requested, saved, structure_options={"column.vector." + key: value})
        raw.mlp.Clear()
        raw.autodis.num_channels = 3
        self.module.prepare_config(requested, saved, structure_options={"column.vector.autodis.num_channels": "3"})
        with self.assertRaisesRegex(ValueError, "autodis.num_channels"):
            self.module.prepare_config(requested, saved, structure_options={"column.vector.autodis.num_channels": "4"})
        raw.boundaries.extend([.1, .5])
        self.module.prepare_config(requested, saved, structure_options={"column.vector.boundaries": ".1, .5"})
        with self.assertRaisesRegex(ValueError, "boundaries"):
            self.module.prepare_config(requested, saved, structure_options={"column.vector.boundaries": ".2, .5"})

    def test_dssm_overrides_do_not_compare_the_other_tower_defaults(self):
        saved, requested = self.config(), self.config()
        saved.model_config.Clear()
        dssm = saved.model_config.dssm
        saved.model_config.losses.add().softmax_cross_entropy.SetInParent()
        dssm.user_tower.input, dssm.item_tower.input = "user", "item"
        dssm.user_tower.mlp.hidden_units.extend([8, 4])
        dssm.item_tower.mlp.hidden_units.extend([16, 8])
        dssm.output_dim = 8
        feature = saved.feature_configs.add().id_feature
        feature.feature_name, feature.expression = "other", "item:other"
        feature.embedding_dim, feature.num_buckets = 16, 100
        saved.model_config.feature_groups.add(group_name="user", feature_names=["id"])
        saved.model_config.feature_groups.add(group_name="item", feature_names=["other"])
        options = {"user_hidden_units": "8,4", "item_hidden_units": "16,8", "output_dim": "8",
                   "user_features": "id", "item_features": "other"}
        effective = self.module.prepare_config(requested, saved, structure_options=options)
        self.assertEqual(effective.model_config, saved.model_config)
        self.module.prepare_config(requested, saved, structure_options={"user_features": ""})
        for key in options:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                self.module.prepare_config(requested, saved, structure_options={key: "32"})

    def test_dssm_recall_checkpoint_and_runtime_overrides_are_preserved(self):
        saved, requested = self.config("dssm"), self.config("dssm")
        saved.model_config.dssm.output_dim = 8
        requested.data_config.batch_size = 16
        effective = self.module.prepare_config(requested, saved)
        self.assertEqual(effective.model_config, saved.model_config)
        self.assertEqual(effective.data_config.batch_size, 16)
        self.assertEqual(requested.model_config.dssm.output_dim, 0)

    def test_dssm_unsupported_request_and_saved_objectives_cannot_be_hidden_by_merge(self):
        for objective in ("binary_cross_entropy", "l2_loss"):
            unsupported = self.config("dssm")
            getattr(unsupported.model_config.losses[0], objective).SetInParent()
            for requested, saved in ((unsupported, None),
                                     (unsupported, self.config("dssm")),
                                     (self.config("dssm"), unsupported),
                                     (self.config(), unsupported),
                                     (unsupported, self.config())):
                with self.subTest(objective=objective, saved=saved), self.assertRaisesRegex(
                        ValueError, "DSSM requires exactly one softmax_cross_entropy"):
                    self.module.prepare_config(requested, saved)

    def test_dssm_requires_one_supported_loss(self):
        config = self.config("dssm")
        for count in (0, 1, 2):
            del config.model_config.losses[:]
            for _ in range(count):
                loss = config.model_config.losses.add()
                if count == 2:
                    loss.softmax_cross_entropy.SetInParent()
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, "DSSM requires exactly one"):
                self.module.prepare_config(config)

    def test_raw_defaults_compare_numeric_values_and_keep_checkpoint_text(self):
        saved, requested = self.config(), self.config()
        raw = saved.feature_configs.add().raw_feature
        raw.feature_name, raw.expression = "price", "item:price"
        for text in ("0.0", "+0e0", "-0", " 0 "):
            with self.subTest(text=text):
                effective = self.module.prepare_config(requested, saved,
                    structure_options={"column.price.default_value": text})
                self.assertEqual(effective.feature_configs, saved.feature_configs)
        raw.value_dim, raw.default_value = 2, "0\0350.1"
        self.module.prepare_config(requested, saved,
            structure_options={"column.price.default_value": "0.0\0350.10000000149"})
        for text in ("", "0", "0\0350.2", "0\035NaN", "0\0351e40"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.module.prepare_config(requested, saved,
                    structure_options={"column.price.default_value": text})

    def test_normalizers_compare_parameter_values_and_effective_defaults(self):
        saved, requested = self.config(), self.config()
        raw = saved.feature_configs.add().raw_feature
        raw.feature_name, raw.expression = "price", "item:price"
        for original, equivalent, changed in (
            ("method=zscore,mean=0,standard_deviation=1",
             "standard_deviation=1.0, method=zscore, mean=0e0",
             "method=zscore,mean=1,standard_deviation=1"),
            ("method=minmax,min=0.1,max=1",
             "max=1e0,min=0.10000000149,method=minmax",
             "method=minmax,min=0.2,max=1"),
            ("method=log10", "default=-1e1,method=log10,threshold=1e-10",
             "method=log10,default=-4"),
        ):
            raw.normalizer = original
            with self.subTest(original=original):
                effective = self.module.prepare_config(requested, saved,
                    structure_options={"column.price.normalizer": equivalent})
                self.assertEqual(effective.feature_configs, saved.feature_configs)
                with self.assertRaisesRegex(ValueError, "normalizer"):
                    self.module.prepare_config(requested, saved,
                        structure_options={"column.price.normalizer": changed})
        raw.normalizer = "method=zscore,mean=0,standard_deviation=1"
        for invalid in ("method=zscore,mean=0,mean=0,standard_deviation=1",
                        "method=zscore,mean=0,standard_deviation=NaN",
                        "method=zscore,mean=0,standard_deviation=1,extra=0"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.module.prepare_config(requested, saved,
                    structure_options={"column.price.normalizer": invalid})

    def test_id_defaults_and_separators_still_require_exact_matches(self):
        saved, requested = self.config(), self.config()
        feature = saved.feature_configs[0].id_feature
        feature.ClearField("num_buckets")
        feature.hash_bucket_size, feature.default_value, feature.separator = 100, "0", "|"
        self.module.prepare_config(requested, saved, structure_options={"column.id.default_value": "0"})
        for option, value in (("default_value", "0.0"), ("default_value", "00"), ("separator", ",")):
            with self.subTest(option=option, value=value), self.assertRaisesRegex(ValueError, option):
                self.module.prepare_config(requested, saved, structure_options={"column.id." + option: value})

    def test_mmoe_accepts_only_equivalent_raw_feature_format_changes(self):
        saved = self.config("mmoe")
        raw = saved.feature_configs.add().raw_feature
        raw.feature_name, raw.expression = "price", "item:price"
        raw.normalizer = "method=zscore,mean=0,standard_deviation=1"
        requested = self.Config()
        requested.CopyFrom(saved)
        requested.feature_configs[1].raw_feature.default_value = "0.0"
        requested.feature_configs[1].raw_feature.normalizer = "standard_deviation=1.0,mean=0,method=zscore"
        effective = self.module.prepare_config(requested, saved)
        self.assertEqual(effective.feature_configs, saved.feature_configs)
        for field, value in (("default_value", "1"), ("normalizer", "method=zscore,mean=1,standard_deviation=1"),
                             ("value_dim", 2), ("separator", "|")):
            changed = self.Config()
            changed.CopyFrom(requested)
            setattr(changed.feature_configs[1].raw_feature, field, value)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "Checkpoint MMoE"):
                self.module.prepare_config(changed, saved)
        requested.feature_configs[0].id_feature.default_value = "0"
        with self.assertRaisesRegex(ValueError, "Checkpoint MMoE"):
            self.module.prepare_config(requested, saved)


if __name__ == "__main__":
    unittest.main()
