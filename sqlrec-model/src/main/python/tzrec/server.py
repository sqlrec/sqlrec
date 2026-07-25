import argparse
import os
from typing import Any

import torch
import pyarrow as pa
from pyarrow import Array

from tzrec.datasets.data_parser import DataParser
from tzrec.features.feature import create_features
from tzrec.utils import config_util
from tzrec.utils.filesystem_util import url_to_fs, apply_monkeypatch
from tzrec.constant import Mode
from tzrec.utils.logging_util import logger


_model: torch.jit.ScriptModule = None
_data_parser: DataParser = None
_device: torch.device = None


def columnar_to_row(data: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = list(data.keys())
    if not keys:
        return []
    
    lengths = [len(v) for v in data.values()]
    unique_lengths = set(lengths)
    
    if len(unique_lengths) == 1:
        n = lengths[0]
    else:
        if 1 not in unique_lengths:
            raise ValueError("All lists in columnar format must have the same length or some can have length 1")
        non_1_lengths = [l for l in unique_lengths if l != 1]
        if len(non_1_lengths) != 1:
            raise ValueError("All non-length-1 lists in columnar format must have the same length")
        n = non_1_lengths[0]
    
    result = []
    for i in range(n):
        row = {}
        for key in keys:
            if len(data[key]) == 1:
                row[key] = data[key][0]
            else:
                row[key] = data[key][i]
        result.append(row)
    return result


def parse_request_data(request_data: Any) -> list[dict[str, Any]]:
    if isinstance(request_data, list):
        return request_data
    if isinstance(request_data, dict):
        for key, value in request_data.items():
            if not isinstance(value, list):
                raise ValueError("Map values must be lists")
        return columnar_to_row(request_data)
    raise ValueError("Input data must be a list of JSON objects or a map with string keys and list values")


def json_to_array_map(data: list[dict[str, Any]]) -> dict[str, Array]:
    table = pa.Table.from_pylist(data)
    input_data: dict[str, pa.Array] = {}
    for field_name in table.schema.names:
        input_data[field_name] = table.column(field_name).combine_chunks()
    return input_data


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
    global _model, _data_parser, _device
    _device = get_device()
    logger.info(f"Loading model from {scripted_model_path}")
    apply_monkeypatch()
    fs, local_path = url_to_fs(scripted_model_path)
    if fs is not None:
        # scripted model use io in cpp, so that we can not path to fsspec
        local_path = os.environ.get("LOCAL_CACHE_DIR", local_path)
        if int(os.environ.get("LOCAL_RANK", 0)) == 0:
            logger.info(f"downloading {scripted_model_path} to {local_path}.")
            fs.download(scripted_model_path, local_path, recursive=True)
        scripted_model_path = local_path
    _model = torch.jit.load(
        os.path.join(scripted_model_path, "scripted_model.pt"), map_location=_device
    )
    _model.eval()
    pipeline_config = config_util.load_pipeline_config(
        os.path.join(scripted_model_path, "pipeline.config"), allow_unknown_field=True
    )
    features = create_features(list(pipeline_config.feature_configs), pipeline_config.data_config.fg_mode)
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
    with torch.no_grad():
        predictions = _model(parsed_data, _device)
    result = {}
    for key, value in predictions.items():
        if isinstance(value, torch.Tensor):
            result[key] = value.cpu().numpy().tolist()
    return result


import flask
app = flask.Flask(__name__)


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

        input_data = json_to_array_map(request_data)
        result = _forward(input_data)
        return flask.jsonify(result)
    except ValueError as e:
        return flask.jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
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
