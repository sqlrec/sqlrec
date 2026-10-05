"""Supported native RocketLaunching configuration and light prediction contract."""


def rocket_contract(config):
    """Validate the supported native branch without changing the model factory."""
    model = config.model_config
    if model.WhichOneof("model") != "rocket_launching":
        return None
    from tzrec.protos import model_pb2, simi_pb2
    network = model.rocket_launching
    widths = [list(network.booster_mlp.hidden_units), list(network.light_mlp.hidden_units)]
    if network.HasField("share_mlp"):
        widths.append(list(network.share_mlp.hidden_units))
    if any(not units or any(unit <= 0 for unit in units) for units in widths):
        raise ValueError("RocketLaunching requires nonempty positive hidden units")
    if not set(widths[0]) & set(widths[1]):
        raise ValueError("RocketLaunching booster and light need a matching hidden width")
    if not network.feature_based_distillation or network.feature_distillation_function != simi_pb2.COSINE:
        raise ValueError("RocketLaunching requires enabled COSINE feature distillation")
    if (model.num_class != 1 or len(model.losses) != 1
            or model.losses[0].WhichOneof("loss") != "binary_cross_entropy"
            or model.use_pareto_loss_weight):
        raise ValueError("RocketLaunching requires one scalar BCE objective without Pareto weighting")
    if len(config.data_config.label_fields) != 1 or config.data_config.WhichOneof("sampler") is not None:
        raise ValueError("RocketLaunching requires one label and paired rows without a negative sampler")
    groups = model.feature_groups
    names = [getattr(feature, feature.WhichOneof("feature")).feature_name for feature in config.feature_configs]
    if (len(groups) != 1 or groups[0].group_type != model_pb2.DEEP or not names or len(set(names)) != len(names)
            or list(groups[0].feature_names) != names or groups[0].sequence_groups or groups[0].sequence_encoders):
        raise ValueError("RocketLaunching requires one DEEP group covering its non-sequence features")
    if set(config.data_config.label_fields) & set(names) or any(name.lower() in ("probs_light", "logits_light") for name in names):
        raise ValueError("RocketLaunching features cannot contain labels or light output names")
    return {"architecture": "rocket_launching", "output_fields": [{"name": "probs_light", "type": "FLOAT"}]}


def validate_predictions(predictions, batch_size):
    """Require native light-only finite scalar logits and probabilities."""
    import torch
    if set(predictions) != {"probs_light", "logits_light"}:
        raise ValueError("RocketLaunching predictions must contain only light probabilities and logits")
    for name in ("probs_light", "logits_light"):
        value = predictions[name]
        if (not isinstance(value, torch.Tensor) or not value.is_floating_point()
                or list(value.shape) != [batch_size] or not torch.isfinite(value).all().item()):
            raise ValueError(f"Invalid RocketLaunching output {name}: expected {batch_size} finite scalar predictions")
    probability = predictions["probs_light"]
    if torch.any((probability < 0) | (probability > 1)).item():
        raise ValueError("RocketLaunching probabilities must be in [0,1]")


def validate_scripted_export(directory, contract):
    """Execute a real export without labels and reject remaining booster parameters."""
    import json
    from pathlib import Path
    import torch
    from tzrec.constant import Mode
    from tzrec.datasets.data_parser import DataParser
    from tzrec.features.feature import create_features
    from tzrec.utils import config_util
    from request_data import json_to_array_map

    root = Path(directory)
    config = config_util.load_pipeline_config(str(root / "pipeline.config"))
    if rocket_contract(config) != contract:
        raise ValueError("Exported RocketLaunching pipeline does not match requested output contract")
    specs = {feature["expression"].split(":", 1)[1]: feature
             for feature in json.loads((root / "fg.json").read_text())["features"]}
    row = {key: None if spec["feature_type"] == "raw_feature" else "0" for key, spec in specs.items()}
    parser = DataParser(create_features(list(config.feature_configs), config.data_config.fg_mode),
                        labels=[], sample_weights=[], mode=Mode.PREDICT, fg_threads=1)
    model = torch.jit.load(str(root / "scripted_model.pt"), map_location="cpu").eval()
    if any("booster" in name for name, _ in model.named_parameters()):
        raise ValueError("RocketLaunching export still contains booster parameters")
    with torch.inference_mode():
        for size in (1, 2, 8):
            inputs = parser.parse(json_to_array_map([row] * size, input_specs=specs))
            validate_predictions(model(inputs, torch.device("cpu")), size)
