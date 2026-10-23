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


def _check_structure_options(saved, options, masks):
    """Compare explicit SQL overrides with checkpoint values, ignoring other defaults."""
    architecture = saved.model_config.WhichOneof("model")
    network = getattr(saved.model_config, architecture)
    features = {getattr(feature, feature.WhichOneof("feature")).feature_name: feature
                for feature in saved.feature_configs}
    for key, value in options.items():
        if key in ("embedding_dim", "num_buckets"):
            for name, feature in features.items():
                if name in masks.get(key, []):
                    continue
                kind = feature.WhichOneof("feature")
                spec = getattr(feature, kind)
                if key == "embedding_dim" and spec.embedding_dim:
                    actual = spec.embedding_dim
                elif key == "num_buckets" and kind == "id_feature":
                    actual = spec.num_buckets if spec.HasField("num_buckets") else spec.hash_bucket_size
                else:
                    continue
                if actual != int(value):
                    raise ValueError(f"Explicit structure override {key} does not match checkpoint feature {name}")
            continue
        if key.startswith("column."):
            matches = [(name, feature) for name, feature in features.items() if key.startswith("column." + name + ".")]
            if not matches:
                raise ValueError(f"Explicit structure override {key} references a feature absent from the checkpoint")
            name, feature = max(matches, key=lambda match: len(match[0]))
            kind = feature.WhichOneof("feature")
            spec = getattr(feature, kind)
            option = key[len("column." + name + "."):]
            if option == "bucket_size" and kind == "id_feature":
                actual = spec.num_buckets if spec.HasField("num_buckets") else spec.hash_bucket_size
                expected = int(value)
            elif option in ("embedding_dim", "value_dim"):
                actual, expected = getattr(spec, option), int(value)
            elif option == "boundaries" and kind == "raw_feature":
                actual = list(spec.boundaries)
                expected = [_float32(part) for part in value.split(",")]
            elif option == "embedding" and kind == "raw_feature":
                actual, expected = spec.WhichOneof("dense_emb") or "none", value
            elif option == "autodis.num_channels" and kind == "raw_feature" and spec.HasField("autodis"):
                actual, expected = spec.autodis.num_channels, int(value)
            elif option == "default_value" and kind == "raw_feature":
                actual, expected = _raw_defaults(spec.default_value, spec.separator), _raw_defaults(value, spec.separator)
            elif option == "normalizer" and kind == "raw_feature":
                actual, expected = _normalizer_values(spec.normalizer), _normalizer_values(value)
            elif option in ("default_value", "separator", "normalizer") and option in spec.DESCRIPTOR.fields_by_name:
                actual, expected = getattr(spec, option), value
            else:
                raise ValueError(f"Explicit structure override {key} is incompatible with the checkpoint feature")
        elif key == "hidden_units" and architecture in ("wide_and_deep", "deepfm"):
            actual, expected = list(network.deep.hidden_units), [int(part) for part in value.split(",")]
        elif key in ("user_hidden_units", "item_hidden_units") and architecture == "dssm":
            tower = network.user_tower if key == "user_hidden_units" else network.item_tower
            actual, expected = list(tower.mlp.hidden_units), [int(part) for part in value.split(",")]
        elif key in ("user_features", "item_features") and architecture == "dssm":
            tower = network.user_tower if key == "user_features" else network.item_tower
            actual = next(list(group.feature_names) for group in saved.model_config.feature_groups if group.group_name == tower.input)
            expected = [part.strip() for part in value.split(",") if part.strip()]
            if not expected:
                other = network.item_tower if key == "user_features" else network.user_tower
                other_features = next(set(group.feature_names) for group in saved.model_config.feature_groups if group.group_name == other.input)
                expected = [name for name in features if name not in other_features]
        elif key == "output_dim" and architecture == "dssm":
            actual, expected = network.output_dim, int(value)
        elif key == "negative_sampling" and architecture == "dssm":
            actual = "pool" if saved.data_config.HasField("pool_negative_sampler") else "in_batch"
            expected = value
        elif key in ("negative_sample_count", "negative_pool_size", "item_id_column") and architecture == "dssm":
            if not saved.data_config.HasField("pool_negative_sampler"):
                raise ValueError(f"Explicit structure override {key} requires a pool checkpoint")
            field = {"negative_sample_count": "num_sample", "negative_pool_size": "pool_size",
                     "item_id_column": "item_id_field"}[key]
            actual = getattr(saved.data_config.pool_negative_sampler, field)
            expected = value if key == "item_id_column" else int(value)
        else:
            raise ValueError(f"Explicit structure override {key} is incompatible with checkpoint model {architecture}")
        if actual != expected:
            raise ValueError(f"Explicit structure override {key} does not match the checkpoint; create a new model to change its structure")


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
    return structure


def prepare_config(requested, saved=None, check_checkpoint_structure=False, structure_options=None, structure_masks=None,
                   training=False):
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
        if architecture == "rocket_launching":
            if (requested_structure != saved_structure
                    or not _same_feature_configs(requested.feature_configs, saved.feature_configs)):
                raise ValueError("Checkpoint RocketLaunching model and features cannot change; create a new model")
        elif architecture == "mmoe":
            if (task_contract(requested) != task_contract(saved)
                    or requested_structure != saved_structure
                    or not _same_feature_configs(requested.feature_configs, saved.feature_configs)):
                raise ValueError("Checkpoint MMoE task, model and feature configuration cannot be changed; create a new model")
        elif structure_options:
            _check_structure_options(saved, structure_options, structure_masks or {})
        elif check_checkpoint_structure and (requested_structure != saved_structure
                                            or not _same_feature_configs(requested.feature_configs, saved.feature_configs)):
            raise ValueError("Explicit network and feature overrides must match the checkpoint; create a new model to change its structure")
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
        del config.feature_configs[:]
        config.feature_configs.extend(saved.feature_configs)
        batch_size, workers = config.data_config.batch_size, config.data_config.num_workers
        config.data_config.CopyFrom(saved.data_config)
        config.data_config.batch_size, config.data_config.num_workers = batch_size, workers
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
