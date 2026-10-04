import argparse
import os
import json
import threading
from typing import Any

import torch
import pyarrow as pa
from pyarrow import Array
from werkzeug.exceptions import HTTPException

from request_data import json_to_array_map, parse_request_data

from tzrec.datasets.data_parser import DataParser
from tzrec.features.feature import create_features
from tzrec.utils import config_util
from tzrec.constant import Mode
from tzrec.utils.logging_util import logger
from validate_export import validate_export


_model: torch.jit.ScriptModule | None = None
_data_parser: DataParser | None = None
_device: torch.device | None = None
_id_inputs: set[str] = set()
_input_specs: dict[str, dict] = {}
_inference_lock = threading.Lock()


def get_device() -> torch.device:
    rank = int(os.environ.get("LOCAL_RANK", 0))
    if torch.cuda.is_available():
        device: torch.device = torch.device(f"cuda:{rank}")
        torch.cuda.set_device(device)
    else:
        device: torch.device = torch.device("cpu")
    return device


def _init_model(scripted_model_path: str) -> None:
    """Initialize model and data parser."""
    global _model, _data_parser, _device, _id_inputs, _input_specs
    device = get_device()
    logger.info(f"Loading model from {scripted_model_path}")
    if "://" in scripted_model_path:
        raise ValueError("server.py requires a local model directory; use server.sh for remote exports")
    pipeline_config = config_util.load_pipeline_config(
        os.path.join(scripted_model_path, "pipeline.config"), allow_unknown_field=True
    )
    validate_export(scripted_model_path, list(pipeline_config.data_config.label_fields))
    model = torch.jit.load(
        os.path.join(scripted_model_path, "scripted_model.pt"), map_location=device
    )
    model.eval()
    features = create_features(list(pipeline_config.feature_configs), pipeline_config.data_config.fg_mode)
    with open(os.path.join(scripted_model_path, "fg.json")) as f:
        fg = json.load(f)
    input_specs = {config["expression"].split(":", 1)[1]: config for config in fg["features"]}
    id_inputs = {
        config.id_feature.expression.split(":", 1)[1]
        for config in pipeline_config.feature_configs
        if config.HasField("id_feature")
    }
    data_parser = DataParser(
        features,
        labels=[],
        sample_weights=[],
        mode=Mode.PREDICT,
        fg_threads=1,
    )
    warmup = {key: None if spec["feature_type"] == "raw_feature" else "0" for key, spec in input_specs.items()}
    parsed = data_parser.parse(json_to_array_map([warmup], input_specs=input_specs))
    with torch.inference_mode():
        model({key: value.to(device) for key, value in parsed.items()}, device)
    with _inference_lock:
        _model, _data_parser, _device, _input_specs, _id_inputs = model, data_parser, device, input_specs, id_inputs
    logger.info("Model initialized successfully")


def _forward(input_data: dict[str, Array]) -> dict[str, Any]:
    parsed_data = _data_parser.parse(input_data)
    return _run_model(parsed_data)


def _run_model(parsed_data: dict[str, torch.Tensor]) -> dict[str, Any]:
    parsed_data = {k: v.to(_device) for k, v in parsed_data.items()}
    with torch.inference_mode():
        predictions = _model(parsed_data, _device)
    result = {}
    for key, value in predictions.items():
        if not isinstance(value, torch.Tensor) or value.is_complex():
            raise RuntimeError("Model outputs must be real tensors")
        if value.is_floating_point() and not torch.isfinite(value).all():
            raise RuntimeError("Model produced nonfinite output")
        result[key] = value.detach().cpu().tolist()
    return result


import flask
from flask.json.provider import DefaultJSONProvider


class StrictJSONProvider(DefaultJSONProvider):
    """Reject non-standard NaN/Infinity even in unused request columns."""

    def loads(self, value, **kwargs):
        def reject_constant(token):
            raise ValueError(f"Invalid JSON number: {token}")
        kwargs["parse_constant"] = reject_constant
        return super().loads(value, **kwargs)


app = flask.Flask(__name__)
app.json = StrictJSONProvider(app)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


@app.route("/health", methods=["GET"])
def health():
    """Match the native readiness endpoint used by Kubernetes probes."""
    if _model is None or _data_parser is None:
        return flask.jsonify({"error": "Model not initialized"}), 503
    return flask.jsonify({"status": "ok"}), 200


@app.route("/predict", methods=["POST"])
def predict():
    """HTTP predict endpoint."""
    if _model is None or _data_parser is None:
        return flask.jsonify({"error": "Model not initialized"}), 500

    if flask.request.mimetype != "application/json":
        return flask.jsonify({"error": "Content-Type must be application/json"}), 400
    try:
        request_data = flask.request.get_json()
        request_data = parse_request_data(request_data)
        if len(request_data) == 0:
            return flask.jsonify({"error": "Input data is empty"}), 400

        if len(request_data) > 4096:
            raise ValueError("Batch size exceeds 4096")
        input_data = json_to_array_map(request_data, input_specs=_input_specs)
        with _inference_lock:
            try:
                parsed_data = _data_parser.parse(input_data)
                if sum(value.numel() for key, value in parsed_data.items() if key.endswith(".values")) > 65536:
                    raise ValueError("Feature value count exceeds 65536")
            except (ValueError, TypeError, pa.ArrowException) as e:
                raise ValueError(str(e)) from e
            try:
                return flask.jsonify(_run_model(parsed_data))
            except Exception:
                logger.exception("Prediction error")
                return flask.jsonify({"error": "Model prediction failed"}), 500
    except ValueError as e:
        return flask.jsonify({"error": str(e)}), 400
    except HTTPException as e:
        return flask.jsonify({"error": e.description}), 413 if e.code == 413 else 400
    except Exception as e:
        logger.exception("Prediction error")
        return flask.jsonify({"error": "Model prediction failed"}), 500


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scripted_model_dir",
        type=str,
        required=True,
        help="path to the exported scripted model directory",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="host to bind the server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=80,
        help="port to bind the server",
    )
    args = parser.parse_args()

    _init_model(args.scripted_model_dir)

    app.run(host=args.host, port=args.port, threaded=False)
