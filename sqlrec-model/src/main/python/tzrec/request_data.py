"""Convert prediction requests into row and Arrow column formats."""

from typing import Any, Collection
import math
import re

import pyarrow as pa
from pyarrow import Array


def columnar_to_row(data: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not data:
        return []
    row_count = max(map(len, data.values()))
    if row_count > 4096:
        raise ValueError("Batch size exceeds 4096")
    if any(len(values) not in (1, row_count) for values in data.values()):
        raise ValueError("Column lengths must match or be 1 for broadcasting")
    return [
        {key: values[0] if len(values) == 1 else values[i]
         for key, values in data.items()}
        for i in range(row_count)
    ]


def parse_request_data(request_data: Any) -> list[dict[str, Any]]:
    if isinstance(request_data, list):
        if not all(isinstance(row, dict) for row in request_data):
            raise ValueError("Every input row must be a JSON object")
        return request_data
    if isinstance(request_data, dict):
        if not all(isinstance(value, list) for value in request_data.values()):
            raise ValueError("Map values must be lists")
        return columnar_to_row(request_data)
    raise ValueError("Input data must be a list of JSON objects or a map with string keys and list values")


def json_to_array_map(
    data: list[dict[str, Any]], id_inputs: Collection[str] = (),
    input_specs: dict[str, dict] | None = None,
) -> dict[str, Array]:
    columns = {key: [] for row in data for key in row}
    required = set(input_specs) if input_specs is not None else set(id_inputs)
    missing = required - columns.keys()
    if missing:
        raise ValueError(f"Missing input columns: {', '.join(sorted(missing))}")
    if required:
        columns = {key: columns[key] for key in columns if key in required}
    for row in data:
        for key, values in columns.items():
            values.append(row.get(key))
    result = {}
    for key, values in columns.items():
        if input_specs is not None:
            spec = input_specs[key]
            raw = spec["feature_type"] == "raw_feature"
            if raw:
                dim = spec.get("value_dim", 1)
                separator = spec.get("separator", "\x1d")
                normalized = []
                for value in values:
                    if value is None or value == "" or value == []:
                        normalized.append(None)
                        continue
                    tokens = value.split(separator) if isinstance(value, str) else value if isinstance(value, list) else [value]
                    if len(tokens) != dim:
                        raise ValueError(f"{key}: value count must match value_dim={dim}")
                    converted = []
                    for token in tokens:
                        if isinstance(token, bool) or not isinstance(token, (str, int, float)):
                            raise ValueError(f"{key}: expected a number or numeric string")
                        if isinstance(token, str) and not re.fullmatch(r"\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*", token):
                            raise ValueError(f"{key}: expected a complete decimal number")
                        try:
                            number = float(token)
                        except (ValueError, OverflowError) as error:
                            raise ValueError(f"{key}: number must be finite float32") from error
                        if not math.isfinite(number) or abs(number) > float.fromhex("0x1.fffffep+127"):
                            raise ValueError(f"{key}: number must be finite float32")
                        converted.append(number)
                    normalized.append(converted[0] if dim == 1 else converted)
                result[key] = pa.array(normalized, type=pa.float32() if dim == 1 else pa.list_(pa.float32()))
                continue
            scalar = []
            has_list = any(isinstance(value, list) for value in values)
            for value in values:
                if value is None or value == "" or value == []:
                    scalar.append(None if value is None else [] if has_list else "")
                    continue
                tokens = value if isinstance(value, list) else [value]
                if any(isinstance(token, bool) or not isinstance(token, (int, str)) for token in tokens):
                    raise ValueError(f"{key}: expected an integer, string or string list")
                if isinstance(value, list) and any(not isinstance(token, str) or not token for token in tokens):
                    raise ValueError(f"{key}: list IDs must be nonempty strings")
                if any(isinstance(token, int) and not -(2**63) <= token < 2**64 for token in tokens):
                    raise ValueError(f"{key}: ID is outside the JSON integer range")
                if has_list:
                    scalar.append(value if isinstance(value, list) else str(value).split(spec.get("separator", "\x1d")))
                else:
                    scalar.append(str(value))
            result[key] = pa.array(scalar, type=pa.list_(pa.string()) if has_list else pa.string())
            continue
        array = pa.array(values)
        if key in id_inputs:
            if pa.types.is_null(array.type):
                array = pa.array(values, type=pa.string())
            elif pa.types.is_list(array.type) and pa.types.is_null(array.type.value_type):
                array = pa.array(values, type=pa.list_(pa.string()))
        result[key] = array
    return result
