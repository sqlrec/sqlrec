"""Verify native DSSM recall targets in the matching TZRec model image."""

import unittest
from pathlib import Path

import torch
import torch.nn.functional as F
from torchrec import KeyedTensor

from tzrec.datasets.utils import BASE_DATA_GROUP, Batch
from tzrec.features.feature import create_features
from tzrec.models.dssm import DSSM
from tzrec.protos import data_pb2, feature_pb2, loss_pb2, model_pb2, module_pb2, tower_pb2
from tzrec.protos.models import match_model_pb2
from tzrec.utils.state_dict_util import init_parameters


class DssmRecallTest(unittest.TestCase):
    def test_default_recall_cutoffs_distinguish_ranks_with_32_candidates(self):
        from tzrec_pipeline_smoke import config as smoke_config

        config = smoke_config(Path("/unused"), "dssm")
        model = DSSM(config.model_config,
                     create_features(list(config.feature_configs), config.data_config.fg_mode),
                     list(config.data_config.label_fields), sampler_type=None)
        model.init_loss()
        model.init_metric()
        similarity = torch.arange(32, dtype=torch.float).expand(32, -1)
        batch = Batch(labels={"label": torch.ones(32, dtype=torch.long)})
        predictions = {"similarity": similarity}
        loss = {"softmax_cross_entropy": F.cross_entropy(similarity, torch.arange(32))}
        model.update_metric(predictions, batch, loss)
        model.update_train_metric(predictions, batch)
        evaluated, trained = model.compute_metric(), model.compute_train_metric()
        for top_k in (1, 5, 10, 20):
            with self.subTest(top_k=top_k):
                self.assertAlmostEqual(evaluated[f"recall@{top_k}"].item(), top_k / 32)
                self.assertAlmostEqual(trained[f"recall@{top_k}"].item(), top_k / 32)

    def test_softmax_keeps_matching_layout_and_ignores_label_values(self):
        features = create_features([
            feature_pb2.FeatureConfig(raw_feature=feature_pb2.RawFeature(feature_name=name, expression="item:" + name))
            for name in ("u", "i")
        ], fg_mode=data_pb2.FG_NONE)
        config = model_pb2.ModelConfig(
            feature_groups=[model_pb2.FeatureGroupConfig(group_name=name, feature_names=[feature],
                group_type=model_pb2.DEEP) for name, feature in (("user", "u"), ("item", "i"))],
            dssm=match_model_pb2.DSSM(
                user_tower=tower_pb2.Tower(input="user", mlp=module_pb2.MLP(hidden_units=[4], use_bn=False)),
                item_tower=tower_pb2.Tower(input="item", mlp=module_pb2.MLP(hidden_units=[4], use_bn=False)),
                output_dim=2, in_batch_negative=True),
            losses=[loss_pb2.LossConfig(softmax_cross_entropy=loss_pb2.SoftmaxCrossEntropy())],
        )
        model = DSSM(config, features, ["label"], sampler_type=None)
        init_parameters(model, device=torch.device("cpu"))
        model.eval()
        model.init_loss()
        values = torch.tensor([[.2, .7], [.8, .3]])
        batch = Batch(dense_features={BASE_DATA_GROUP: KeyedTensor.from_tensor_list(
            keys=["u", "i"], tensors=[values[:, :1], values[:, 1:]])},
            labels={"label": torch.zeros(2, dtype=torch.long)})
        prediction = model(batch)
        self.assertEqual(prediction["similarity"].shape, (2, 2))
        expected = F.cross_entropy(prediction["similarity"], torch.arange(2))
        loss = model.loss(prediction, batch)["softmax_cross_entropy"]
        torch.testing.assert_close(loss, expected)
        batch.labels["label"] = torch.ones(2, dtype=torch.long)
        torch.testing.assert_close(model.loss(prediction, batch)["softmax_cross_entropy"], loss)


if __name__ == "__main__":
    unittest.main()
