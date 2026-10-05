import importlib.util
import json
from pathlib import Path
import sys
import types
import tempfile
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

    def test_sql_null_text_returns_400_before_tokenization(self):
        from test_huggingface_classification import ClassificationTest
        adapter = ClassificationTest().adapter(None, [[2., 3.]])
        self.server._adapter = adapter
        response = self.client.post("/predict", json=[{"body": None}])
        self.assertEqual(response.status_code, 400, response.text)
        self.assertIn("non-null STRING", response.json()["detail"])
        adapter.tokenizer.assert_not_called()

    def test_checkpoint_task_must_match_declared_service_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_SUCCESS").touch()
            (root / "sqlrec_manifest.json").write_text(json.dumps({
                "task": "embedding", "model_params": {"text_column": "text"},
            }))
            config = root / "service.config"
            config.write_text(json.dumps({"task": "text-classification"}))
            with self.assertRaisesRegex(RuntimeError, "checkpoint task does not match"):
                self.server.load_runtime(directory, str(config))

    def test_matching_checkpoint_task_loads_adapter(self):
        calls = []

        def adapter(directory, config):
            calls.append(config)
            return types.SimpleNamespace()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_SUCCESS").touch()
            (root / "sqlrec_manifest.json").write_text(json.dumps({
                "task": "embedding", "model_params": {"text_column": "text"},
            }))
            config = root / "service.config"
            config.write_text(json.dumps({"task": "embedding", "inference_batch_size": 3}))
            with patch.dict(self.server.TASK_ADAPTERS, {"embedding": adapter}):
                self.server.load_runtime(directory, str(config))
        self.assertEqual(calls[0]["task"], "embedding")
        self.assertEqual(calls[0]["text_column"], "text")
        self.assertEqual(self.server._batch_size, 3)

    def test_legacy_checkpoint_column_options_use_declared_schema_spelling(self):
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_SUCCESS").touch()
            (root / "sqlrec_manifest.json").write_text(json.dumps({
                "task": "text-classification",
                "input_fields": [{"name": "Body", "type": "STRING"}, {"name": "Title", "type": "STRING"}],
                "model_params": {"text_column": "BODY", "text_pair_column": "TITLE"},
            }))
            config = root / "service.config"
            config.write_text(json.dumps({"text_column": "body"}))
            with patch.dict(self.server.TASK_ADAPTERS, {
                    "text-classification": lambda directory, config: calls.append(config)}):
                self.server.load_runtime(directory, str(config))
        self.assertEqual(calls[0]["text_column"], "Body")
        self.assertEqual(calls[0]["text_pair_column"], "Title")
