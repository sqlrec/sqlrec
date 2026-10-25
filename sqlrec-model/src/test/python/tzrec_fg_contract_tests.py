"""Exercise real TZRec/FG boundary contracts in the pinned model images."""

import sys
import unittest

import pyarrow as pa

sys.path.insert(0, "/app")

from request_data import json_to_array_map
from tzrec.constant import Mode
from tzrec.datasets.data_parser import DataParser
from tzrec.features.feature import create_features
from tzrec.protos import data_pb2, feature_pb2

from tzrec_feature_assertions import assert_feature_tensors


def parse(feature, column):
    features = create_features([feature], fg_mode=data_pb2.FG_NORMAL)
    return DataParser(features, labels=[], mode=Mode.PREDICT, fg_threads=1).parse({"input": column})


class FgContractTest(unittest.TestCase):
    def test_log10_omitted_parameters_and_encoded_defaults(self):
        for normalizer, expected in (
                ("method=log10", [0, 0, 0, 0, 1, 0.25]),
                ("method=log10,threshold=0.05", [0, 0, -1, 0, 1, 0.25]),
                ("method=log10,default=-5", [-5, -5, -5, -5, 1, 0.25])):
            feature = feature_pb2.FeatureConfig(raw_feature=feature_pb2.RawFeature(
                feature_name="raw", expression="item:input", default_value="0.25", normalizer=normalizer))
            with self.subTest(normalizer=normalizer):
                tensors = parse(feature, pa.array([0, 0.05, 0.1, 1, 10, None], type=pa.float32()))
                assert_feature_tensors({"values": tensors["raw.values"].tolist()},
                                       {"values": [[value] for value in expected]}, dense=True, context=normalizer)

    def test_hash_default_is_one_token_for_strings_and_lists(self):
        feature = feature_pb2.FeatureConfig(id_feature=feature_pb2.IdFeature(
            feature_name="id", expression="item:input", hash_bucket_size=100,
            embedding_dim=8, separator="|", default_value="1|2"))
        for column in (pa.array(["3|4", "", None], type=pa.string()),
                       pa.array([["3", "4"], [], None], type=pa.list_(pa.string()))):
            with self.subTest(dtype=column.type):
                tensors = parse(feature, column)
                assert_feature_tensors({suffix: tensors[f"id.{suffix}"].tolist() for suffix in ("values", "lengths")},
                                       {"values": [21, 43, 11, 11], "lengths": [2, 1, 1]}, dense=False, context="ID default")

    def test_scalar_empty_bucket_default_uses_numeric_zero_without_normalization(self):
        raw = feature_pb2.RawFeature(feature_name="raw", expression="item:input", default_value="",
                                    boundaries=[-1, 0, 1], normalizer="method=zscore,mean=10,standard_deviation=1")
        feature = feature_pb2.FeatureConfig(raw_feature=raw)
        spec = {"input": {"feature_type": "raw_feature", "default_value": "", "boundaries": [-1, 0, 1]}}
        column = json_to_array_map([{"input": 0}, {"input": None}, {"input": ""}, {"input": []}, {}], spec)["input"]
        self.assertEqual(column.type, pa.float32())
        tensors = parse(feature, column)
        assert_feature_tensors({suffix: tensors[f"raw.{suffix}"].tolist() for suffix in ("values", "lengths")},
                               {"values": [0, 2, 2, 2, 2], "lengths": [1, 1, 1, 1, 1]}, dense=False, context="scalar raw null")

    def test_vector_empty_bucket_default_retains_empty_rows(self):
        feature = feature_pb2.FeatureConfig(raw_feature=feature_pb2.RawFeature(
            feature_name="raw", expression="item:input", value_dim=2, default_value="", boundaries=[0, 1]))
        tensors = parse(feature, pa.array([[0, 1], [], None], type=pa.list_(pa.float32())))
        assert_feature_tensors({suffix: tensors[f"raw.{suffix}"].tolist() for suffix in ("values", "lengths")},
                               {"values": [1, 2], "lengths": [2, 0, 0]}, dense=False, context="vector raw null")


if __name__ == "__main__":
    unittest.main()
