"""Exercise TensorBoard persistence, fine-tuning and resume with real training."""

import argparse
import json
import math
import os
from pathlib import Path
import sys
import tempfile

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import text_format
from tensorboard.backend.event_processing.event_accumulator import EventAccumulator
from tzrec.constant import TRAIN_EVAL_RESULT_FILENAME
from tzrec.protos import pipeline_pb2
from tzrec.utils import config_util

sys.path.insert(0, "/app")
from tzrec_pipeline_smoke import execute


def fixture(directory, data, *, tensorboard=True, metrics=False, source=None):
    config = text_format.Parse('''
      data_config { dataset_type: ParquetDataset fg_mode: FG_NORMAL
        batch_size: 8 num_workers: 0 label_fields: "label" }
      train_config { num_epochs: 2 log_step_count_steps: 1
        save_checkpoints_steps: 0 save_checkpoints_epochs: 1
        sparse_optimizer { adagrad_optimizer { lr: 0.01 } constant_learning_rate {} }
        dense_optimizer { adam_optimizer { lr: 0.01 } constant_learning_rate {} }
      }
      feature_configs { id_feature { feature_name: "id" expression: "item:id"
        num_buckets: 32 embedding_dim: 8 } }
      model_config {
        feature_groups { group_name: "wide" group_type: WIDE feature_names: "id" }
        feature_groups { group_name: "deep" group_type: DEEP feature_names: "id" }
        wide_and_deep { deep { hidden_units: [8, 4] } }
        losses { binary_cross_entropy {} }
        metrics { auc {} }
      }
    ''', pipeline_pb2.EasyRecConfig())
    config.model_dir = str(directory)
    config.train_input_path = config.eval_input_path = str(data)
    config.eval_config.SetInParent()
    config.train_config.use_tensorboard = tensorboard
    config.train_config.tensorboard_summaries.extend(("loss", "learning_rate", "global_gradient_norm"))
    if source:
        config.train_config.fine_tune_checkpoint = str(source)
    if metrics:
        config.model_config.metrics.add().normalized_entropy.SetInParent()
        config.model_config.metrics.add().accuracy.SetInParent()
        config.model_config.train_metrics.add(decay_step=2, decay_rate=.9).auc.SetInParent()
        config.model_config.train_metrics.add(decay_step=2, decay_rate=.9).accuracy.SetInParent()
    return config


def read_events(directory, *, training_metrics):
    fs, path = fsspec.core.url_to_fs(directory)
    assert fs.glob(path + "/events.out.tfevents.*"), directory
    train = EventAccumulator(directory, size_guidance={"scalars": 0}).Reload()
    validation = EventAccumulator(directory + "/eval_val", size_guidance={"scalars": 0}).Reload()
    assert {"loss/total", "lr/g0"} <= set(train.Tags()["scalars"]), train.Tags()
    assert "global_gradient_norm" in train.Tags()["scalars"], train.Tags()
    assert ("metric/auc" in train.Tags()["scalars"]) == training_metrics, train.Tags()
    assert ("metric/accuracy" in train.Tags()["scalars"]) == training_metrics, train.Tags()
    assert "metric/normalized_entropy" not in train.Tags()["scalars"], train.Tags()
    assert {"metric/auc", "metric/binary_cross_entropy"} <= set(validation.Tags()["scalars"])
    if training_metrics:
        assert {"metric/normalized_entropy", "metric/accuracy"} <= set(validation.Tags()["scalars"])
    assert len({point.step for point in train.Scalars("loss/total")}) >= 2
    assert len(validation.Scalars("metric/auc")) == 2
    for accumulator in (train, validation):
        for tag in accumulator.Tags()["scalars"]:
            assert all(math.isfinite(point.value) for point in accumulator.Scalars(tag)), tag
    with fs.open(path + "/" + TRAIN_EVAL_RESULT_FILENAME) as stream:
        results = [json.loads(line) for line in stream]
    for tag in validation.Tags()["scalars"]:
        points = validation.Scalars(tag)
        for point, result in zip(points, results):
            assert point.step == result["global_step"]
            assert math.isclose(point.value, result[tag.removeprefix("metric/")], rel_tol=1e-5, abs_tol=1e-6)
    return train


def verify_native_defaults(root, data, launcher, env):
    config = fixture(root / "native_defaults", data)
    config.train_config.num_steps = 104
    for field in ("num_epochs", "use_tensorboard", "log_step_count_steps",
                  "save_checkpoints_steps", "save_checkpoints_epochs", "tensorboard_summaries"):
        config.train_config.ClearField(field)
    metric = config.model_config.train_metrics.add()
    metric.auc.SetInParent()
    assert not metric.HasField("decay_step") and not metric.HasField("decay_rate")
    path = root / "native_defaults.config"
    config_util.save_message(config, str(path))
    execute(launcher + ["/app/run.py", "--mode", "train", "--pipeline_config_path", str(path)],
            root / "native_defaults.log", env)
    accumulator = EventAccumulator(config.model_dir, size_guidance={"scalars": 0}).Reload()
    assert {"loss/total", "lr/g0", "metric/auc"} <= set(accumulator.Tags()["scalars"])
    points = accumulator.Scalars("metric/auc")
    assert [point.step for point in points] == [0, 100], points
    assert points[0].value == 0 and points[1].value > 0, points
    result = {"case": "native_default_collection", "training_steps": 104,
              "metric_steps": [point.step for point in points]}
    (root / "native-default-result.json").write_text(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir")
    parser.add_argument("--remote-root", help="New remote directory for a persistence check")
    args = parser.parse_args()
    temporary = tempfile.TemporaryDirectory() if not args.work_dir else None
    root = Path(args.work_dir or temporary.name)
    root.mkdir(parents=True, exist_ok=True)
    data = root / "data.parquet"
    pq.write_table(pa.Table.from_pylist([{"id": i % 16, "label": i % 2} for i in range(32)]), data)
    env = {**os.environ, "USE_FSSPEC": "1", "USE_SPAWN_MULTI_PROCESS": "1",
           "USE_FARM_HASH_TO_BUCKETIZE": "true", "TORCH_MANUAL_SEED": "123", "NUMPY_MANUAL_SEED": "123"}
    launcher = ["torchrun", "--standalone", "--nnodes=1", "--nproc-per-node=1"]
    summary = []

    def train(name, *, source=None, tensorboard=True, metrics=False, directory=None, input_path=data):
        directory = str(directory or root / name)
        config = fixture(directory, input_path, tensorboard=tensorboard, metrics=metrics, source=source)
        path = root / (name + ".config")
        config_util.save_message(config, str(path))
        execute(launcher + ["/app/run.py", "--mode", "train", "--pipeline_config_path", str(path)],
                root / (name + ".log"), env)
        fs, native = fsspec.core.url_to_fs(directory)
        if tensorboard:
            accumulator = read_events(directory, training_metrics=metrics)
            if metrics:
                points = accumulator.Scalars("metric/auc")
                if source:
                    previous = EventAccumulator(str(source)).Reload()
                    if "metric/auc" in previous.Tags()["scalars"]:
                        assert math.isclose(points[0].value, previous.Scalars("metric/auc")[-1].value, abs_tol=1e-6)
                    else:
                        assert points[0].value == 0
                else:
                    assert points[0].value == 0
                assert points[-1].value > 0
        else:
            assert not fs.glob(native + "/events.out.tfevents.*"), directory
            assert fs.exists(native + "/" + TRAIN_EVAL_RESULT_FILENAME)
        summary.append({"case": name, "model_dir": directory, "tensorboard": tensorboard, "training_metrics": metrics})
        return config

    disabled = train("tensorboard_disabled", tensorboard=False)
    legacy = train("legacy_without_train_metrics")
    enabled = train("enable_from_legacy", source=legacy.model_dir, metrics=True)
    train("native_fine_tune_metric_restore", source=enabled.model_dir, metrics=True)
    train("disable_from_enabled", source=enabled.model_dir)
    summary.append(verify_native_defaults(root, data, launcher, env))

    for config in (disabled, enabled):
        execute(launcher + ["-m", "tzrec.eval", "--pipeline_config_path", config.model_dir + "/pipeline.config"],
                root / (Path(config.model_dir).name + "-evaluate.log"), env)
        events = Path(config.model_dir) / "eval"
        assert list(events.glob("events.out.tfevents.*")), events
        accumulator = EventAccumulator(str(events)).Reload()
        # Native standalone eval does not close its writer; async summaries may remain unwritten.
        for tag in accumulator.Tags()["scalars"]:
            assert all(math.isfinite(point.value) for point in accumulator.Scalars(tag)), tag
        results = [json.loads(line) for line in (Path(config.model_dir) / "eval_result.txt").read_text().splitlines()]
        assert results and all(math.isfinite(result["auc"]) for result in results), results
    summary.append({"case": "native_standalone_evaluation"})

    accumulator = EventAccumulator(enabled.model_dir).Reload()
    last = accumulator.Scalars("metric/auc")[-1]
    enabled.train_config.num_epochs = 3
    enabled.train_config.ClearField("fine_tune_checkpoint")
    resume = root / "resume.config"
    config_util.save_message(enabled, str(resume))
    execute(launcher + ["-m", "tzrec.train_eval", "--pipeline_config_path", str(resume), "--continue_train"],
            root / "resume.log", env)
    resumed = EventAccumulator(enabled.model_dir, size_guidance={"scalars": 0}).Reload()
    points = [point for point in resumed.Scalars("metric/auc") if point.step > last.step]
    assert points and math.isclose(points[0].value, last.value, abs_tol=1e-6), points
    summary.append({"case": "continue_train_preserves_metric_history", "last_step": points[-1].step})

    if args.remote_root:
        remote = args.remote_root.rstrip("/")
        fs, path = fsspec.core.url_to_fs(remote)
        if fs.exists(path):
            raise FileExistsError(remote)
        fs.makedirs(path)
        fs.put(str(data), path + "/data.parquet")
        train("remote_persistence", directory=remote + "/model", input_path=remote + "/data.parquet", metrics=True)
    result = {"cases": summary}
    (root / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    if temporary:
        temporary.cleanup()


if __name__ == "__main__":
    main()
