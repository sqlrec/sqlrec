"""Protect exact sparse indices and the dense comparison contract."""

import unittest

from tzrec_feature_assertions import assert_feature_tensors


class FeatureAssertionsTest(unittest.TestCase):
    def test_adjacent_large_ids_and_lengths_are_not_equal(self):
        for suffix in ("values", "lengths"):
            for baseline in (1000000, 2**60):
                expected = {"values": [baseline], "lengths": [baseline]}
                actual = {**expected, suffix: [baseline + 1]}
                with self.subTest(suffix=suffix, baseline=baseline), self.assertRaises(AssertionError):
                    assert_feature_tensors(actual, expected, dense=False, context="sparse")

    def test_dense_rounding_is_allowed_but_shape_and_nonfinite_values_are_not(self):
        expected = {"values": [[1.0, -0.0]]}
        assert_feature_tensors({"values": [[1.0000005, -0.0]]}, expected, dense=True, context="dense")
        for values in ([1.0, -0.0], [[float("nan"), -0.0]], [[float("inf"), -0.0]], [[1.0, 0.0]]):
            with self.subTest(values=values), self.assertRaises(AssertionError):
                assert_feature_tensors({"values": values}, expected, dense=True, context="dense")


if __name__ == "__main__":
    unittest.main()
