import argparse
import os
from typing import Any

import torch
from pyarrow import Array

from request_data import json_to_array_map, parse_request_data

from tzrec.datasets.data_parser import DataParser
from tzrec.features.feature import create_features
from tzrec.utils import config_util
from tzrec.constant import Mode
from tzrec.utils.logging_util import logger


_model: torch.jit.ScriptModule | None = None
_data_parser: DataParser | None = None
_device: torch.device | None = None
_id_inputs: set[str] = set()


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
    global _model, _data_parser, _device, _id_inputs
    _device = get_device()
    logger.info(f"Loading model from {scripted_model_path}")
    if "://" in scripted_model_path:
        raise ValueError("server.py requires a local model directory; use server.sh for remote exports")
    _model = torch.jit.load(
        os.path.join(scripted_model_path, "scripted_model.pt"), map_location=_device
    )
    _model.eval()
    pipeline_config = config_util.load_pipeline_config(
        os.path.join(scripted_model_path, "pipeline.config"), allow_unknown_field=True
    )
    features = create_features(list(pipeline_config.feature_configs), pipeline_config.data_config.fg_mode)
    _id_inputs = {
        config.id_feature.expression.split(":", 1)[1]
        for config in pipeline_config.feature_configs
        if config.HasField("id_feature")
    }
    _data_parser = DataParser(
        features,
        labels=[],
        sample_weights=[],
        mode=Mode.PREDICT,
        fg_threads=1,
    )
    logger.info("Model initialized successfully")


def _forward(input_data: dict[str, Array]) -> dict[str, Any]:
    parsed_data = _data_parser.parse(input_data)
    parsed_data = {k: v.to(_device) for k, v in parsed_data.items()}
    with torch.inference_mode():
        predictions = _model(parsed_data, _device)
    result = {}
    for key, value in predictions.items():
        if isinstance(value, torch.Tensor):
            result[key] = value.cpu().numpy().tolist()
    return result


import flask
app = flask.Flask(__name__)


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

    try:
        request_data = flask.request.get_json()
        request_data = parse_request_data(request_data)
        if len(request_data) == 0:
            return flask.jsonify({"error": "Input data is empty"}), 400

        input_data = json_to_array_map(request_data, _id_inputs)
        result = _forward(input_data)
        return flask.jsonify(result)
    except ValueError as e:
        return flask.jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception("Prediction error")
        return flask.jsonify({"error": str(e)}), 500


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

    app.run(host=args.host, port=args.port)
