"""Checkpoint-owned pipeline configuration, independent of the training runtime."""

import math
import os
import re
import struct
from urllib.parse import urlsplit

from multi_task import task_contract
from rocket_launching_contract import rocket_contract


def _float32(value):
    """Resolve decimal configuration values at the feature generator's precision."""
    if not re.fullmatch(r"\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*", value):
        raise ValueError("Expected a decimal float32 number")
    number = float(value)
    if not math.isfinite(number) or abs(number) > float.fromhex("0x1.fffffep+127"):
        raise ValueError("Number must be finite float32")
    return struct.unpack("f", struct.pack("f", number))[0]


def _raw_defaults(text, separator):
    """Compare raw defaults as ordered float32 values without normalization."""
    return tuple(_float32(value) for value in text.split(separator)) if text else ()


def _normalizer_values(text):
    """Resolve supported normalizers independently of parameter order and spelling."""
    if not text:
        return "", {}
    parts = {}
    for part in text.split(","):
        pair = part.strip().split("=")
        if len(pair) != 2 or pair[0] in parts:
            raise ValueError("Invalid normalizer")
        parts[pair[0]] = pair[1]
    method = parts.pop("method", "")
    required = {"zscore": {"mean", "standard_deviation"}, "minmax": {"min", "max"},
                "log10": {"threshold", "default"}}.get(method)
    if method == "log10":
        parts.setdefault("threshold", "1")
        parts.setdefault("default", "0")
    if required is None or set(parts) != required:
        raise ValueError("Invalid normalizer parameters")
    values = {key: _float32(value) for key, value in parts.items()}
    if (method == "zscore" and values["standard_deviation"] <= 0
            or method == "minmax" and values["max"] <= values["min"]
            or method == "log10" and values["threshold"] <= 0):
        raise ValueError("Normalizer scale must be positive")
    return method, values


def _same_feature_configs(requested, saved):
    """Allow equivalent raw value formatting while comparing other fields exactly."""
    if len(requested) != len(saved):
        return False
    for current, previous in zip(requested, saved):
        if current == previous:
            continue
        if current.WhichOneof("feature") != "raw_feature" or previous.WhichOneof("feature") != "raw_feature":
            return False
        left, right = current.raw_feature, previous.raw_feature
        if (_raw_defaults(left.default_value, left.separator) != _raw_defaults(right.default_value, right.separator)
                or _normalizer_values(left.normalizer) != _normalizer_values(right.normalizer)):
            return False
        comparable = type(current)()
        comparable.CopyFrom(current)
        for field in ("default_value", "normalizer"):
            if right.HasField(field):
                setattr(comparable.raw_feature, field, getattr(right, field))
            else:
                comparable.raw_feature.ClearField(field)
        if comparable != previous:
            return False
    return True


def _check_dssm_loss(config):
    """Reject unsupported DSSM objectives before checkpoint configuration is merged."""
    if config.model_config.WhichOneof("model") != "dssm":
        return
    losses = config.model_config.losses
    if len(losses) != 1 or losses[0].WhichOneof("loss") != "softmax_cross_entropy":
        raise ValueError("SQLRec DSSM requires exactly one softmax_cross_entropy loss for recall; "
                         "other objectives and their checkpoints are unsupported")


def _model_structure(model):
    """Compare checkpoint structure independently of runtime metrics."""
    structure = type(model)()
    structure.CopyFrom(model)
    structure.ClearField("metrics")
    structure.ClearField("train_metrics")
    if structure.WhichOneof("model") == "mmoe":
        for tower in structure.mmoe.task_towers:
            tower.ClearField("metrics")
            tower.ClearField("train_metrics")
            tower.ClearField("weight")
    if structure.WhichOneof("model") == "dssm":
        structure.dssm.ClearField("in_batch_negative")
    return structure


def prepare_config(requested, saved=None, training=False):
    """Retain checkpoint structure and validate the effective label contract."""
    _check_dssm_loss(requested)
    rocket_contract(requested)
    config = type(requested)()
    config.CopyFrom(requested)
    if saved is not None:
        _check_dssm_loss(saved)
        rocket_contract(saved)
        if list(config.data_config.label_fields) != list(saved.data_config.label_fields):
            raise ValueError("Checkpoint label_fields cannot be changed during export or fine-tuning")
        architecture = requested.model_config.WhichOneof("model")
        if architecture != saved.model_config.WhichOneof("model"):
            raise ValueError("Checkpoint architecture must match the declared model; create a new model")
        requested_structure = _model_structure(requested.model_config)
        saved_structure = _model_structure(saved.model_config)
        if (requested_structure != saved_structure
                or not _same_feature_configs(requested.feature_configs, saved.feature_configs)):
            raise ValueError("Checkpoint model, features and task types must match the MODEL definition")
        if training and architecture == "dssm" and requested.model_config.dssm.in_batch_negative != saved.model_config.dssm.in_batch_negative:
            raise ValueError("FROM cannot change the DSSM negative sampling mode")
        config.model_config.CopyFrom(saved.model_config)
        if training:
            del config.model_config.metrics[:]
            config.model_config.metrics.extend(requested.model_config.metrics)
            del config.model_config.train_metrics[:]
            config.model_config.train_metrics.extend(requested.model_config.train_metrics)
            if architecture == "mmoe":
                for tower, current in zip(config.model_config.mmoe.task_towers, requested.model_config.mmoe.task_towers):
                    del tower.metrics[:]
                    tower.metrics.extend(current.metrics)
                    del tower.train_metrics[:]
                    tower.train_metrics.extend(current.train_metrics)
                    tower.weight = current.weight
        del config.feature_configs[:]
        config.feature_configs.extend(saved.feature_configs)
        if not training:
            batch_size = requested.data_config.batch_size
            config.data_config.CopyFrom(saved.data_config)
            config.data_config.batch_size = batch_size
            config.data_config.num_workers = 1
    if training and config.data_config.HasField("pool_negative_sampler"):
        if not config.train_input_path:
            raise ValueError("Pool sampling requires a parquet training input")
        config.data_config.pool_negative_sampler.input_path = config.train_input_path
    contract = task_contract(config)
    if training:
        if config.train_config.is_profiling and urlsplit(config.model_dir).scheme not in ("", "file"):
            raise ValueError("Native profiling requires a local or mounted model_dir; remote URIs are unsupported")
        log_steps = config.train_config.log_step_count_steps
        metrics = list(config.model_config.train_metrics)
        if config.model_config.WhichOneof("model") == "mmoe":
            metrics.extend(metric for tower in config.model_config.mmoe.task_towers for metric in tower.train_metrics)
        if log_steps <= 0:
            raise ValueError("log_step_count_steps must be positive")
        for metric in metrics:
            if metric.decay_step <= 0 or metric.decay_step % log_steps != 0:
                raise ValueError("Training metric decay_step must be a positive multiple of log_step_count_steps")
            if not math.isfinite(metric.decay_rate) or not 0 <= metric.decay_rate < 1:
                raise ValueError("Training metric decay_rate must be finite and in [0,1)")
    labels = set(config.data_config.label_fields)
    if contract is None and (len(config.data_config.label_fields) != 1 or len(labels) != 1):
        raise ValueError("TZRec requires exactly one label")
    for feature in config.feature_configs:
        name = feature.WhichOneof("feature")
        spec = getattr(feature, name)
        if spec.feature_name in labels or spec.expression.split(":", 1)[-1] in labels:
            raise ValueError("Checkpoint contains label features; retrain with labels excluded")
        if name not in ("id_feature", "raw_feature"):
            raise ValueError(f"Unsupported SQLRec serving feature: {name}")
        if name == "id_feature" and spec.HasField("hash_bucket_size") and os.getenv("USE_FARM_HASH_TO_BUCKETIZE", "true").lower() != "true":
            raise ValueError("Hash features require USE_FARM_HASH_TO_BUCKETIZE=true")
    return config
