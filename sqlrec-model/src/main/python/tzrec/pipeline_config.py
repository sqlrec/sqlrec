"""Checkpoint-owned pipeline configuration, independent of the training runtime."""

import os

from multi_task import task_contract


def prepare_config(requested, saved=None):
    """Retain checkpoint structure and validate the effective label contract."""
    config = type(requested)()
    config.CopyFrom(requested)
    if saved is not None:
        if list(config.data_config.label_fields) != list(saved.data_config.label_fields):
            raise ValueError("Checkpoint label_fields cannot be changed during export or fine-tuning")
        if (requested.model_config.WhichOneof("model") == "mmoe"
                or saved.model_config.WhichOneof("model") == "mmoe"):
            if (task_contract(requested) != task_contract(saved)
                    or requested.model_config != saved.model_config
                    or requested.feature_configs != saved.feature_configs):
                raise ValueError("Checkpoint MMoE task, model and feature configuration cannot be changed; create a new model")
        config.model_config.CopyFrom(saved.model_config)
        del config.feature_configs[:]
        config.feature_configs.extend(saved.feature_configs)
        batch_size, workers = config.data_config.batch_size, config.data_config.num_workers
        config.data_config.CopyFrom(saved.data_config)
        config.data_config.batch_size, config.data_config.num_workers = batch_size, workers
    contract = task_contract(config)
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
