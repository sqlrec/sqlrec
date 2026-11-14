"""Execution permits belong to a model instance, independently of HTTP protocols."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from huggingface.runtime import ModelBusy, ModelRuntime


class ModelRuntimeTest(unittest.TestCase):
    def test_instances_have_independent_permits(self):
        first = ModelRuntime({})
        second = ModelRuntime({})
        second.adapter = SimpleNamespace(predict=lambda rows: {"value": [row["value"] for row in rows]})
        with first.inference():
            with self.assertRaises(ModelBusy):
                with first.inference():
                    self.fail("An occupied model cannot admit another inference")
            self.assertEqual(second.predict([{"value": 3}]), {"value": [3]})
        with first.inference():
            pass

    def test_failed_native_prediction_releases_the_permit(self):
        runtime = ModelRuntime({})
        predict = Mock(side_effect=[ValueError("invalid input"), {"value": [7]}])
        runtime.adapter = SimpleNamespace(predict=predict)
        with self.assertRaisesRegex(ValueError, "invalid input"):
            runtime.predict([{"value": None}])
        self.assertEqual(runtime.predict([{"value": 7}]), {"value": [7]})


if __name__ == "__main__":
    unittest.main()
