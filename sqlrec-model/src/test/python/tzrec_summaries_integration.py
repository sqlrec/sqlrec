"""Verify native gradient, histogram and profiler collection with real training."""

import argparse
import json
import math
import os
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from tensorboard.backend.event_processing.event_accumulator import EventAccumulator
from tzrec.utils import config_util

from tzrec_pipeline_smoke import config, execute, verify_events


DEFAULT_SUMMARIES = ("loss", "learning_rate", "global_gradient_norm")
ALL_SUMMARIES = (*DEFAULT_SUMMARIES, "gradient_norm", "parameter", "gradient")
ARCHITECTURES = ("wide_and_deep", "deepfm", "dssm", "mmoe", "mmoe_mixed", "mmoe_dense",
                 "mmoe_regression", "rocket_launching", "rocket_launching_shared", "rocket_launching_dense")


def write_data(root):
    rows = [{"category": i % 8, "tags": ["Action", "Comedy" if i % 2 else "Drama"],
             "price": float(i % 32) / 8, "bucket_price": float(i % 32) / 32,
             "vector": [float(i % 32) / 32, float(i % 4) / 4],
             "mlp_price": float(i % 32) / 8, "mlp_vector": [float(i % 32) / 8, float(i % 4) / 4],
             "autodis_price": float(i % 32) / 8, "label": i % 2, "label_like": (i // 2) % 2,
             "watch_time": float(i % 32) / 4, "watch_aux": float(i % 32) / 8} for i in range(128)]
    pq.write_table(pa.Table.from_pylist(rows), root / "data.parquet")
    pq.write_table(pa.Table.from_pylist(rows[:16]), root / "eval.parquet")
    pq.write_table(pa.Table.from_pylist([{**row, "label": 1} for row in rows]), root / "dssm_data.parquet")
    pq.write_table(pa.Table.from_pylist([{**row, "label": 1} for row in rows[:16]]), root / "dssm_eval.parquet")


def verify_summaries(directory, summaries):
    accumulator = EventAccumulator(str(directory), size_guidance={"scalars": 0, "histograms": 0}).Reload()
    tags = accumulator.Tags()
    scalars = set(tags["scalars"])
    assert {"loss/total", "lr/g0"} <= scalars, tags
    assert ("global_gradient_norm" in scalars) == ("global_gradient_norm" in summaries), tags
    gradient_norms = {tag for tag in scalars if tag.endswith("/gradient_norm")}
    assert bool(gradient_norms) == ("gradient_norm" in summaries), tags
    histograms = set(tags["histograms"])
    gradients = {tag for tag in histograms if tag.endswith("/gradient")}
    parameters = histograms - gradients
    assert bool(gradients) == ("gradient" in summaries), tags
    assert bool(parameters) == ("parameter" in summaries), tags
    for tag in scalars:
        points = accumulator.Scalars(tag)
        assert len({point.step for point in points}) >= 2, tag
        assert all(math.isfinite(point.value) for point in points), tag
    if "global_gradient_norm" in summaries:
        assert any(point.value > 0 for point in accumulator.Scalars("global_gradient_norm"))
        if gradient_norms:
            components = {tag: {point.step: point.value for point in accumulator.Scalars(tag)}
                          for tag in gradient_norms}
            for point in accumulator.Scalars("global_gradient_norm"):
                expected = math.sqrt(sum(values[point.step] ** 2 for values in components.values()))
                assert math.isclose(point.value, expected, rel_tol=1e-4, abs_tol=1e-6), (point, expected)
    for tag in histograms:
        points = accumulator.Histograms(tag)
        assert len({point.step for point in points}) >= 2, tag
        for point in points:
            value = point.histogram_value
            assert value.num > 0 and all(math.isfinite(x) for x in
                                        (value.min, value.max, value.sum, value.sum_squares)), tag
    return {"scalars": len(scalars), "parameter_histograms": len(parameters),
            "gradient_histograms": len(gradients), "gradient_norms": len(gradient_norms)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir")
    parser.add_argument("--cases", nargs="+")
    args = parser.parse_args()
    temporary = tempfile.TemporaryDirectory() if not args.work_dir else None
    root = Path(args.work_dir or temporary.name)
    root.mkdir(parents=True, exist_ok=True)
    write_data(root)
    env = {**os.environ, "USE_FARM_HASH_TO_BUCKETIZE": "true", "USE_SPAWN_MULTI_PROCESS": "1",
           "TORCH_MANUAL_SEED": "123", "NUMPY_MANUAL_SEED": "123"}
    cases = [(architecture, architecture, 1, DEFAULT_SUMMARIES, True, False) for architecture in ARCHITECTURES]
    cases.extend([
        ("mmoe_distributed", "mmoe_mixed", 2, DEFAULT_SUMMARIES, True, False),
        ("rocket_distributed", "rocket_launching", 2, DEFAULT_SUMMARIES, True, False),
        ("histograms", "wide_and_deep", 1, ALL_SUMMARIES, True, False),
        ("dense_histograms", "mmoe_dense", 1, ALL_SUMMARIES, True, False),
        ("distributed_histograms", "rocket_launching", 2, ALL_SUMMARIES, True, False),
        ("minimal", "wide_and_deep", 1, ("loss",), True, False),
        ("disabled", "wide_and_deep", 1, ALL_SUMMARIES, False, False),
        ("profiling", "wide_and_deep", 1, DEFAULT_SUMMARIES, True, True),
        ("profiling_without_tensorboard", "wide_and_deep", 1, DEFAULT_SUMMARIES, False, True),
    ])
    if args.cases:
        assert set(args.cases) <= {case[0] for case in cases}, args.cases
        cases = [case for case in cases if case[0] in args.cases]
    result = []
    for name, architecture, processes, summaries, tensorboard, profiling in cases:
        pipeline = config(root, architecture)
        pipeline.model_dir = str(root / name)
        pipeline.train_config.num_epochs = 1
        pipeline.train_config.log_step_count_steps = 1
        pipeline.train_config.save_checkpoints_steps = 8
        pipeline.train_config.use_tensorboard = tensorboard
        pipeline.train_config.is_profiling = profiling
        del pipeline.train_config.tensorboard_summaries[:]
        pipeline.train_config.tensorboard_summaries.extend(summaries)
        if architecture == "dssm":
            pipeline.train_input_path = str(root / "dssm_data.parquet")
            pipeline.eval_input_path = str(root / "dssm_eval.parquet")
        path = root / (name + ".config")
        config_util.save_message(pipeline, str(path))
        launcher = ["torchrun", "--standalone", "--nnodes=1", f"--nproc-per-node={processes}", "/app/run.py"]
        execute(launcher + ["--mode", "train", "--pipeline_config_path", str(path)], root / (name + ".log"), env)
        directory = Path(pipeline.model_dir)
        saved = config_util.load_pipeline_config(str(directory / "pipeline.config"))
        assert list(saved.train_config.tensorboard_summaries) == list(summaries)
        assert saved.train_config.is_profiling == profiling
        if tensorboard:
            assert len(list(directory.glob("events.out.tfevents.*"))) == 1, directory
            observed = verify_summaries(directory, summaries)
            verify_events(pipeline)
        else:
            assert not list(directory.rglob("events.out.tfevents.*")), directory
            observed = {}
        traces = list((directory / "train_eval_trace").glob("*.pt.trace.json"))
        assert bool(traces) == profiling, traces
        for trace in traces:
            payload = json.loads(trace.read_text())
            assert payload.get("traceEvents"), trace
            assert any(event.get("cat") == "cpu_op" for event in payload["traceEvents"]), trace
        item = {"case": name, "processes": processes, "summaries": list(summaries),
                "trace_files": len(traces), **observed}
        result.append(item)
        print(json.dumps(item), flush=True)
    (root / "result.json").write_text(json.dumps(result, indent=2))
    if temporary:
        temporary.cleanup()


if __name__ == "__main__":
    main()
