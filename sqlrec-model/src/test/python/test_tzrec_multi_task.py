"""Validate real Parquet labels without installing the training runtime."""

import importlib.util
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


source = Path("/app/multi_task.py")
if not source.is_file():
    source = Path(__file__).resolve().parents[2] / "main/python/tzrec/multi_task.py"
spec = importlib.util.spec_from_file_location("sqlrec_multi_task", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
TASKS = [{"label": "click", "type": "binary"}, {"label": "watch", "type": "regression"}]


class MultiTaskLabelTest(unittest.TestCase):
    def distributed(self, store, rank):
        torch = ModuleType("torch")
        dist = ModuleType("torch.distributed")
        dist.rendezvous = Mock(return_value=iter([(store, rank, 2)]))
        dist.PrefixStore = Mock(side_effect=lambda prefix, value: value)
        torch.distributed = dist
        return patch.dict("sys.modules", {"torch": torch, "torch.distributed": dist})

    def store(self):
        values = {}
        return SimpleNamespace(set=lambda key, value: values.update({key: value.encode()}), get=lambda key: values[key])

    def test_distributed_validation_scans_only_rank_zero_and_shares_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data.parquet"
            pq.write_table(pa.table({"click": [0, 1], "watch": [1., 2.]}), path)
            config = SimpleNamespace(train_input_path=str(path), eval_input_path=str(path))
            store = self.store()
            with patch.dict("os.environ", {"WORLD_SIZE": "2"}), self.distributed(store, 0), \
                    patch.object(module, "validate_label_paths", wraps=module.validate_label_paths) as scan:
                self.assertIs(module.validate_training_labels(config, {"tasks": TASKS}), store)
                self.assertEqual(scan.call_count, 2)
            config.train_input_path = "/missing/nonzero-rank-must-not-read-this.parquet"
            with patch.dict("os.environ", {"WORLD_SIZE": "2"}), self.distributed(store, 1), \
                    patch.object(module, "validate_label_paths") as scan:
                self.assertIs(module.validate_training_labels(config, {"tasks": TASKS}), store)
                scan.assert_not_called()

    def test_distributed_validation_reports_the_same_error_on_all_ranks(self):
        config = SimpleNamespace(train_input_path="/missing/data.parquet", eval_input_path="")
        store = self.store()
        for rank in (0, 1):
            with patch.dict("os.environ", {"WORLD_SIZE": "2"}), self.distributed(store, rank), \
                    self.assertRaisesRegex(ValueError, "MMoE label validation failed: No Parquet"):
                module.validate_training_labels(config, {"tasks": TASKS})

    def test_streams_only_labels_from_multiple_local_and_remote_files(self):
        rows = [{"click": i % 2, "watch": float(i), "unused": "text"} for i in range(5)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index in range(2):
                pq.write_table(pa.Table.from_pylist(rows), root / f"{index}.parquet")
            module.validate_label_paths(str(root / "*.parquet"), TASKS, batch_size=2)
            module.validate_label_paths(",".join(str(root / f"{index}.parquet") for index in range(2)), TASKS)
        filesystem = fsspec.filesystem("memory")
        with filesystem.open("/sqlrec-label-test/data.parquet", "wb") as stream:
            pq.write_table(pa.Table.from_pylist(rows), stream)
        try:
            module.validate_label_paths("memory:///sqlrec-label-test/*.parquet", TASKS, batch_size=2)
        finally:
            filesystem.rm("/sqlrec-label-test", recursive=True)

    def test_rejects_missing_null_nonnumeric_nonfinite_and_nonbinary_labels(self):
        cases = [({"click": [0, 2], "watch": [1., 2.]}, "binary label click"),
                 ({"click": [0, .5], "watch": [1., 2.]}, "binary label click"),
                 ({"click": [0., 1. + 1e-9], "watch": [1., 2.]}, "binary label click"),
                 ({"click": [0, None], "watch": [1., 2.]}, "label click contains nulls"),
                 ({"click": [0, 1], "watch": [1., float("nan")]}, "label watch must be finite"),
                 ({"click": [0, 1], "watch": [1., float("inf")]}, "label watch must be finite"),
                 ({"click": [0, 1], "watch": [1., 1e40]}, "label watch must be finite"),
                 ({"click": [True, False], "watch": [1., 2.]}, "numeric scalar"),
                 ({"click": ["0", "1"], "watch": [1., 2.]}, "numeric scalar"),
                 ({"click": [0, 1], "watch": [[1.], [2.]]}, "numeric scalar"),
                 ({"click": [0, 1]}, "missing label watch")]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data.parquet"
            for columns, error in cases:
                with self.subTest(columns=columns):
                    pq.write_table(pa.table(columns), path)
                    with self.assertRaisesRegex(ValueError, error):
                        module.validate_label_paths(str(path), TASKS, batch_size=1)

    def test_regression_rejects_integer_storage_and_accepts_float32_and_float64(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data.parquet"
            for dtype in (pa.int32(), pa.int64(), pa.uint64(), pa.float32(), pa.float64()):
                with self.subTest(dtype=dtype):
                    pq.write_table(pa.table({"click": [0, 1], "watch": pa.array([1, 2], type=dtype)}), path)
                    if pa.types.is_integer(dtype):
                        with self.assertRaisesRegex(ValueError, "regression label watch must use a floating Parquet type"):
                            module.validate_label_paths(str(path), TASKS)
                    else:
                        module.validate_label_paths(str(path), TASKS)

    def test_invalid_evaluation_labels_also_prevent_training(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            train, evaluation = root / "train.parquet", root / "eval.parquet"
            pq.write_table(pa.table({"click": [0, 1], "watch": [1., 2.]}), train)
            pq.write_table(pa.table({"click": [0, 1], "watch": [1, 2]}), evaluation)
            config = SimpleNamespace(train_input_path=str(train), eval_input_path=str(evaluation))
            with patch.dict("os.environ", {"WORLD_SIZE": "1"}), \
                    self.assertRaisesRegex(ValueError, "regression label watch"):
                module.validate_training_labels(config, {"tasks": TASKS})

    def test_empty_files_and_unmatched_paths_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "empty.parquet"
            pq.write_table(pa.table({"click": pa.array([], type=pa.int64()), "watch": pa.array([], type=pa.float64())}), path)
            for paths, error in [(str(path), "empty"), (str(path.parent / "missing*.parquet"), "No Parquet"), ("", "requires")]:
                with self.assertRaisesRegex(ValueError, error):
                    module.validate_label_paths(paths, TASKS)


if __name__ == "__main__":
    unittest.main()
