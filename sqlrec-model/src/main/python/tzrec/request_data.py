"""Convert prediction requests into row and Arrow column formats."""

from typing import Any

import pyarrow as pa
from pyarrow import Array


def columnar_to_row(data: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not data:
        return []
    row_count = max(map(len, data.values()))
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


def json_to_array_map(data: list[dict[str, Any]]) -> dict[str, Array]:
    columns = {key: [] for row in data for key in row}
    for row in data:
        for key, values in columns.items():
            values.append(row.get(key))
    return {key: pa.array(values) for key, values in columns.items()}
