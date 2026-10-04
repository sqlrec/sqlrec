"""MMoE task contracts and streaming label validation for SQLRec exports."""

from datetime import timedelta
import json
import math
import os
import re

import fsspec
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def task_contract(config):
    """Resolve supported native task towers into the public ordered contract."""
    if config.model_config.WhichOneof("model") != "mmoe":
        return None
    labels = list(config.data_config.label_fields)
    towers = list(config.model_config.mmoe.task_towers)
    if (len(labels) < 2 or len({label.lower() for label in labels}) != len(labels)
            or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", label) for label in labels)
            or [tower.label_name for tower in towers] != labels
            or [tower.tower_name for tower in towers] != labels):
        raise ValueError("MMoE requires ordered, unique task towers matching label_fields")
    model = config.model_config
    if model.losses or model.metrics or model.train_metrics or model.use_pareto_loss_weight:
        raise ValueError("MMoE requires per-task losses and metrics without Pareto weighting")
    if model.mmoe.num_expert <= 0 or not model.mmoe.expert_mlp.hidden_units:
        raise ValueError("MMoE requires positive experts and a nonempty expert MLP")
    if any(unit <= 0 for unit in model.mmoe.expert_mlp.hidden_units):
        raise ValueError("MMoE expert hidden units must be positive")
    tasks, fields = [], []
    for tower in towers:
        if len(tower.losses) != 1 or tower.num_class != 1:
            raise ValueError(f"{tower.tower_name}: requires one scalar binary or regression loss")
        loss = tower.losses[0].WhichOneof("loss")
        if loss not in ("binary_cross_entropy", "l2_loss"):
            raise ValueError(f"{tower.tower_name}: unsupported task loss {loss}")
        if (not math.isfinite(tower.weight) or tower.weight <= 0
                or not tower.mlp.hidden_units or any(unit <= 0 for unit in tower.mlp.hidden_units)):
            raise ValueError(f"{tower.tower_name}: requires positive weight and hidden units")
        unsupported = {"sample_weight_name", "task_space_indicator_label", "in_task_space_weight",
                       "out_task_space_weight", "pareto_min_loss_weight", "train_metrics"}
        if any(field.name in unsupported for field, _ in tower.ListFields()):
            raise ValueError(f"{tower.tower_name}: unsupported task sampling or weighting option")
        binary = loss == "binary_cross_entropy"
        allowed = {"auc", "accuracy"} if binary else {"mean_squared_error", "mean_absolute_error"}
        metrics = [metric.WhichOneof("metric") for metric in tower.metrics]
        if not metrics or len(set(metrics)) != len(metrics) or not set(metrics) <= allowed:
            raise ValueError(f"{tower.tower_name}: metrics do not match task loss")
        tasks.append({"name": tower.tower_name, "label": tower.label_name,
                      "type": "binary" if binary else "regression"})
        fields.append({"name": ("probs_" if binary else "y_") + tower.tower_name, "type": "FLOAT"})
    return {"tasks": tasks, "output_fields": fields}


def validate_label_paths(paths, tasks, batch_size=65536):
    """Scan only label columns, rejecting absent, null and invalid scalar labels."""
    if not paths:
        raise ValueError("MMoE requires Parquet training data")
    labels = [task["label"] for task in tasks]
    total_rows = 0
    for pattern in paths.split(","):
        filesystem, path = fsspec.core.url_to_fs(pattern.strip())
        files = sorted(filesystem.glob(path))
        if not files:
            raise ValueError(f"No Parquet files found: {pattern}")
        for filename in files:
            if not filesystem.isfile(filename):
                raise ValueError(f"Expected a Parquet file or file glob: {filename}")
            with filesystem.open(filename, "rb") as stream:
                parquet = pq.ParquetFile(stream)
                for task in tasks:
                    label = task["label"]
                    if label not in parquet.schema_arrow.names:
                        raise ValueError(f"{filename}: missing label {label}")
                    dtype = parquet.schema_arrow.field(label).type
                    if not (pa.types.is_integer(dtype) or pa.types.is_floating(dtype)):
                        raise ValueError(f"{filename}: label {label} must be a numeric scalar")
                    # TZRec preserves integer label tensors; native L2 backward requires floats.
                    if task["type"] == "regression" and not pa.types.is_floating(dtype):
                        raise ValueError(f"{filename}: regression label {label} must use a floating Parquet type")
                for batch in parquet.iter_batches(batch_size=batch_size, columns=labels):
                    total_rows += batch.num_rows
                    for task in tasks:
                        label = task["label"]
                        values = batch.column(batch.schema.get_field_index(label))
                        if values.null_count:
                            raise ValueError(f"{filename}: label {label} contains nulls")
                        numbers = values.cast(pa.float32(), safe=False)
                        if not pc.all(pc.is_finite(numbers)).as_py():
                            raise ValueError(f"{filename}: label {label} must be finite float32")
                        if task["type"] == "binary" and not pc.all(pc.or_(pc.equal(values, 0), pc.equal(values, 1))).as_py():
                            raise ValueError(f"{filename}: binary label {label} must be 0 or 1")
    if total_rows == 0:
        raise ValueError("MMoE label data is empty")


def validate_training_labels(config, contract):
    """Validate on rank zero and share the outcome through torchrun's rendezvous store.

    Keep the returned store alive until training completes: non-agent rendezvous
    servers are shared with TZRec's later process-group initialization.
    """
    def scan():
        validate_label_paths(config.train_input_path, contract["tasks"])
        if config.eval_input_path:
            validate_label_paths(config.eval_input_path, contract["tasks"])

    if int(os.getenv("WORLD_SIZE", "1")) == 1:
        scan()
        return None
    import torch.distributed as dist
    timeout = timedelta(seconds=int(os.getenv("PROCESS_GROUP_TIMEOUT_SECONDS", "1800")))
    store, rank, _ = next(dist.rendezvous("env://", timeout=timeout))
    store = dist.PrefixStore("sqlrec-label-validation-" + os.getenv("TORCHELASTIC_RESTART_COUNT", "0"), store)
    if rank == 0:
        error = None
        try:
            scan()
        except Exception as exception:
            error = str(exception)
        store.set("result", json.dumps(error))
    error = json.loads(store.get("result"))
    if error is not None:
        raise ValueError(f"MMoE label validation failed: {error}")
    return store


def validate_predictions(predictions, contract, batch_size):
    """Require every public prediction to be a finite floating scalar per row."""
    import torch
    for field in contract["output_fields"]:
        value = predictions.get(field["name"])
        if (not isinstance(value, torch.Tensor) or not value.is_floating_point()
                or list(value.shape) != [batch_size] or not torch.isfinite(value).all().item()):
            raise ValueError(f"Invalid MMoE output {field['name']}: expected {batch_size} finite scalar predictions")


def validate_scripted_export(directory, contract):
    """Run an exported model before publication using only its feature inputs."""
    from pathlib import Path
    import torch
    from tzrec.constant import Mode
    from tzrec.datasets.data_parser import DataParser
    from tzrec.features.feature import create_features
    from tzrec.utils import config_util
    from request_data import json_to_array_map

    root = Path(directory)
    config = config_util.load_pipeline_config(str(root / "pipeline.config"))
    if task_contract(config) != contract:
        raise ValueError("Exported MMoE pipeline does not match requested task contract")
    specs = {feature["expression"].split(":", 1)[1]: feature
             for feature in json.loads((root / "fg.json").read_text())["features"]}
    row = {key: None if spec["feature_type"] == "raw_feature" else "0" for key, spec in specs.items()}
    parser = DataParser(create_features(list(config.feature_configs), config.data_config.fg_mode),
                        labels=[], sample_weights=[], mode=Mode.PREDICT, fg_threads=1)
    model = torch.jit.load(str(root / "scripted_model.pt"), map_location="cpu").eval()
    with torch.inference_mode():
        for size in (1, 2):
            inputs = parser.parse(json_to_array_map([row] * size, input_specs=specs))
            validate_predictions(model(inputs, torch.device("cpu")), contract, size)
