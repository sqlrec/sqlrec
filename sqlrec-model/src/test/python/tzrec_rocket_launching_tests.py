"""Native RocketLaunching gradients, inference graph and checkpoint contracts."""

import sys
import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

sys.path.insert(0, "/app")

import torch
from torchrec import KeyedTensor
from tzrec.datasets.utils import BASE_DATA_GROUP, Batch
from tzrec.features.feature import create_features
from tzrec.models.model import ScriptWrapper
from tzrec.models.rocket_launching import RocketLaunching
from tzrec.protos import data_pb2, feature_pb2, loss_pb2, model_pb2, module_pb2
from tzrec.protos.models import general_rank_model_pb2
from tzrec.utils import config_util
from tzrec.utils.fx_util import symbolic_trace
from tzrec.utils.state_dict_util import init_parameters

from pipeline_config import prepare_config
from run import main
from rocket_launching_contract import rocket_contract, validate_predictions


class RocketLaunchingTest(unittest.TestCase):
    def test_invalid_checkpoint_fails_before_training_or_export(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "checkpoint"
            source.mkdir()
            saved, requested = self.fixture(), self.fixture()
            saved.model_config.rocket_launching.feature_based_distillation = False
            config_util.save_message(saved, str(source / "pipeline.config"))
            requested.model_dir = str(source)
            requested.train_config.fine_tune_checkpoint = str(source)
            path = root / "requested.config"
            config_util.save_message(requested, str(path))
            for mode in ("train", "export"):
                with (self.subTest(mode=mode), patch.dict("os.environ", {"WORLD_SIZE": "1", "RANK": "0"}),
                      patch.object(sys, "argv", ["run.py", "--mode", mode, "--pipeline_config_path", str(path),
                                                 "--export_dir", str(root / "export")]),
                      patch("tzrec.main.train_and_evaluate") as train, patch("tzrec.main.export") as export):
                    with self.assertRaisesRegex(ValueError, "COSINE"):
                        main()
                    train.assert_not_called()
                    export.assert_not_called()
                    self.assertFalse((root / "export").exists())
                    self.assertFalse(list(root.glob("export.staging-*")))

    def fixture(self, name="rocket_launching"):
        return config_util.load_pipeline_config(f"/tests/configs/{name}.config")

    def native_model(self, shared):
        features = create_features([
            feature_pb2.FeatureConfig(raw_feature=feature_pb2.RawFeature(feature_name=name, expression="item:" + name))
            for name in ("u", "i")
        ], fg_mode=data_pb2.FG_NONE)
        config = model_pb2.ModelConfig(
            feature_groups=[model_pb2.FeatureGroupConfig(group_name="deep", feature_names=["u", "i"], group_type=model_pb2.DEEP)],
            losses=[loss_pb2.LossConfig(binary_cross_entropy=loss_pb2.BinaryCrossEntropy())],
            rocket_launching=general_rank_model_pb2.RocketLaunching(
                booster_mlp=module_pb2.MLP(hidden_units=[8, 4], use_bn=False),
                light_mlp=module_pb2.MLP(hidden_units=[4], use_bn=False), feature_based_distillation=True))
        if shared:
            config.rocket_launching.share_mlp.CopyFrom(module_pb2.MLP(hidden_units=[8], use_bn=False))
        torch.manual_seed(123)
        model = RocketLaunching(config, features, ["label"])
        init_parameters(model, device=torch.device("cpu"))
        model.init_loss()
        return model

    def batch(self, size=2):
        values = torch.tensor([[.2, .7], [.8, .3]])[:size]
        return Batch(dense_features={BASE_DATA_GROUP: KeyedTensor.from_tensor_list(
            keys=["u", "i"], tensors=[values[:, :1], values[:, 1:]])},
            labels={"label": torch.tensor([0, 1])[:size]})

    def test_native_gradient_blocks_and_supervised_branches(self):
        for shared in (False, True):
            with self.subTest(shared=shared):
                model = self.native_model(shared)
                model.train()
                batch = self.batch()
                predictions = model(batch)
                losses = model.loss(predictions, batch)
                self.assertEqual(set(losses), {"binary_cross_entropy_booster", "binary_cross_entropy_light", "hint_l2_loss", "similarity_0_1"})
                self.assertIsNone(torch.autograd.grad(losses["hint_l2_loss"], model.booster_linear.weight,
                                                      allow_unused=True, retain_graph=True)[0])
                if shared:
                    weight = next(model.share_mlp.parameters())
                    self.assertIsNone(torch.autograd.grad(losses["binary_cross_entropy_light"], weight,
                                                          allow_unused=True, retain_graph=True)[0])
                    self.assertIsNotNone(torch.autograd.grad(losses["binary_cross_entropy_booster"], weight, retain_graph=True)[0])
                sum(losses.values()).backward()
                self.assertGreater(model.booster_linear.weight.grad.abs().sum().item(), 0)
                self.assertGreater(model.light_linear.weight.grad.abs().sum().item(), 0)
                logits = predictions["logits_light"].detach()
                expected = torch.nn.functional.binary_cross_entropy_with_logits(logits, batch.labels["label"].float())
                torch.testing.assert_close(losses["binary_cross_entropy_light"].detach(), expected)

    def test_light_inference_scripts_without_booster_or_labels(self):
        for shared in (False, True):
            with self.subTest(shared=shared):
                model = self.native_model(shared).eval()
                traced = symbolic_trace(ScriptWrapper(model))
                self.assertFalse(any("booster" in name for name, _ in traced.named_parameters()))
                scripted = torch.jit.script(traced)
                batch = self.batch()
                batch.labels.clear()
                expected = model(batch)
                actual = scripted(batch.to_dict())
                validate_predictions(actual, 2)
                for name in expected:
                    torch.testing.assert_close(actual[name], expected[name])
                single = self.batch(1)
                single.labels.clear()
                prediction = scripted(single.to_dict())
                validate_predictions(prediction, 1)
                torch.testing.assert_close(prediction["probs_light"], actual["probs_light"][:1])

    def test_java_fixtures_and_runtime_checkpoint_changes(self):
        for name in ("rocket_launching", "rocket_launching_shared", "rocket_launching_dense"):
            with self.subTest(name=name):
                saved, requested = self.fixture(name), self.fixture(name)
                contract = rocket_contract(saved)
                self.assertEqual(contract["output_fields"], [{"name": "probs_light", "type": "FLOAT"}])
                requested.data_config.batch_size = 16
                requested.train_config.dense_optimizer.adam_optimizer.lr = .02
                effective = prepare_config(requested, saved)
                self.assertEqual(effective.model_config, saved.model_config)
                self.assertEqual(effective.data_config.batch_size, 16)

    def test_unsupported_native_configs_are_rejected_in_requests_and_checkpoints(self):
        for change in ("disabled", "distance", "widths", "empty_shared", "loss", "classes", "sampler", "wide", "duplicate_features"):
            invalid = self.fixture()
            network = invalid.model_config.rocket_launching
            if change == "disabled": network.feature_based_distillation = False
            elif change == "distance": network.feature_distillation_function = 1
            elif change == "widths": network.light_mlp.hidden_units[:] = [7, 3]
            elif change == "empty_shared": network.share_mlp.SetInParent()
            elif change == "loss": invalid.model_config.losses[0].l2_loss.SetInParent()
            elif change == "classes": invalid.model_config.num_class = 2
            elif change == "sampler": invalid.data_config.negative_sampler.SetInParent()
            elif change == "wide": invalid.model_config.feature_groups[0].group_type = model_pb2.WIDE
            else: invalid.feature_configs.add().CopyFrom(invalid.feature_configs[0])
            for requested, saved in ((invalid, None), (invalid, self.fixture()), (self.fixture(), invalid)):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    prepare_config(requested, saved)

    def test_structure_changes_and_cross_architecture_restore_are_rejected(self):
        for change in ("booster", "shared", "feature", "architecture"):
            saved, requested = self.fixture(), self.fixture()
            if change == "booster": requested.model_config.rocket_launching.booster_mlp.hidden_units[0] = 32
            elif change == "shared": requested.model_config.rocket_launching.share_mlp.hidden_units.append(8)
            elif change == "feature": requested.feature_configs[0].id_feature.embedding_dim = 16
            else: requested.model_config.wide_and_deep.deep.hidden_units.append(8)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "Checkpoint RocketLaunching"):
                prepare_config(requested, saved)
            with self.subTest(change=change, reversed=True), self.assertRaisesRegex(ValueError, "Checkpoint RocketLaunching"):
                prepare_config(saved, requested)

    def test_invalid_prediction_contracts_are_rejected(self):
        valid = {"probs_light": torch.tensor([.2, .3]), "logits_light": torch.tensor([-1., -2.])}
        validate_predictions(valid, 2)
        for invalid in ({"probs_light": valid["probs_light"]}, {**valid, "probs_booster": torch.ones(2)},
                        {**valid, "probs_light": torch.ones(2, 1)}, {**valid, "probs_light": torch.tensor([-1., .5])},
                        {**valid, "logits_light": torch.tensor([float("nan"), 1.])}, {**valid, "probs_light": torch.ones(2, dtype=torch.int64)}):
            with self.assertRaises(ValueError):
                validate_predictions(invalid, 2)


if __name__ == "__main__":
    unittest.main()
