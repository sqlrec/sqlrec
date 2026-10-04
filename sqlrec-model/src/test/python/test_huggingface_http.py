import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


class HuggingFaceHttpTest(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "hf_http_test_server", Path(__file__).resolve().parents[2] / "main/python/huggingface/server.py")
        self.server = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"huggingface.tasks": types.SimpleNamespace(TASK_ADAPTERS={})}):
            spec.loader.exec_module(self.server)
        self.calls = []
        def predict(rows):
            self.calls.append(rows)
            return {"embedding": [[len(row["text"])] for row in rows]}
        self.server._adapter = types.SimpleNamespace(predict=predict)
        self.server._batch_size = 2
        self.client = TestClient(self.server.app)
        self.addCleanup(self.client.close)

    def test_json_body_is_bound_and_predictions_are_batched(self):
        rows = [{"text": "a"}, {"text": "ab"}, {"text": "abc"}]
        response = self.client.post("/predict", json=rows)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"embedding": [[1], [2], [3]]})
        self.assertEqual([len(batch) for batch in self.calls], [2, 1])
        operation = self.client.get("/openapi.json").json()["paths"]["/predict"]["post"]
        self.assertIn("requestBody", operation)
        self.assertNotIn("parameters", operation)

    def test_invalid_inputs_and_unloaded_model_have_explicit_errors(self):
        for body in ([], {}, ["invalid"]):
            with self.subTest(body=body):
                self.assertEqual(self.client.post("/predict", json=body).status_code, 400)
        self.assertEqual(self.calls, [])
        self.server._adapter = None
        self.assertEqual(self.client.post("/predict", json=[{"text": "a"}]).status_code, 503)
