"""Compare real native serving and feature parsing against the installed TZRec FG.

Run in a matching model image with a built features_test executable and local
rank/user/item export directories; this intentionally is not a dependency-free
unit test discovered by Maven.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import numpy as np


def request(url, data=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--feature-parser", required=True)
    parser.add_argument("--python-source", default="/app")
    parser.add_argument("--models", nargs="+", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.python_source)
    import server as reference
    from request_data import json_to_array_map, parse_request_data

    original = {"user_id": 1, "movie_id": 100, "genres": ["Action", "Adventure", "Sci-Fi"],
                "gender": "M", "age": 25, "occupation": 10, "zip_code": "10001"}
    summary = []
    for index, directory in enumerate(args.models):
        directory = Path(directory)
        fg = json.loads((directory / "fg.json").read_text())
        input_names = {f["expression"].split(":", 1)[1] for f in fg["features"]}
        row = {k: v for k, v in original.items() if k in input_names}
        second = {**row}
        if "user_id" in second:
            second["user_id"] = 2
        else:
            second["movie_id"] = 101
        cases = {"sql": [row], "batch2": [row, second], "metadata": [{**row, "rating": 1}]}
        for key in ["gender", "genres"]:
            if key in row:
                cases[f"null_{key}"] = [{**row, key: None}]
                cases[f"empty_{key}"] = [{**row, key: [] if key == "genres" else ""}]
                cases[f"partial_missing_{key}"] = [row, {k: v for k, v in second.items() if k != key}]
        if "genres" in row:
            cases["separated_genres"] = [{**row, "genres": "\x1d".join(row["genres"])}]
        cases["columnar_broadcast"] = {k: [row[k]] if row[k] == second[k] else [row[k], second[k]] for k in row}
        port = 18080 + index
        url = f"http://127.0.0.1:{port}"
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen([args.server, str(directory), "127.0.0.1", str(port)], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    try:
                        assert request(url + "/health")[0] == 200
                        break
                    except urllib.error.URLError:
                        if time.monotonic() > deadline:
                            raise AssertionError("Native server startup timed out")
                        time.sleep(0.1)
                reference._init_model(str(directory))
                result = {"model": str(directory), "cases": {}}
                for name, payload in cases.items():
                    rows = parse_request_data(payload)
                    arrays = json_to_array_map(rows, input_names)
                    expected_tensors = reference._data_parser.parse(arrays)
                    native_features = json.loads(subprocess.check_output(
                        [args.feature_parser, str(directory / "fg.json")], input=json.dumps(rows).encode()))
                    for feature in fg["features"]:
                        key = feature["feature_name"]
                        for suffix in ["values", "lengths"]:
                            assert native_features[key][suffix] == expected_tensors[f"{key}.{suffix}"].tolist(), (name, key, suffix)
                    expected = reference._forward(arrays)
                    status, actual = request(url + "/predict", payload)
                    assert status == 200, (name, actual)
                    assert actual.keys() == expected.keys()
                    max_delta = 0.0
                    for key in expected:
                        assert np.shape(actual[key]) == np.shape(expected[key]), (name, key)
                        np.testing.assert_allclose(actual[key], expected[key], rtol=1e-6, atol=1e-6)
                        max_delta = max(max_delta, float(np.max(np.abs(np.asarray(actual[key]) - expected[key]))))
                    result["cases"][name] = {"max_abs_error": max_delta, "shapes": {k: list(np.shape(v)) for k, v in actual.items()}}
                first_key = next(iter(row))
                invalid = [[], {}, [1], [True], [{k: v for k, v in row.items() if k != first_key}],
                           [{**row, first_key: True}], {first_key: [], "other": [1]}]
                integer_key = "user_id" if "user_id" in row else "movie_id"
                invalid += [[{**row, integer_key: -1}], [{**row, integer_key: 1000000}]]
                for payload in invalid:
                    status, actual = request(url + "/predict", payload)
                    assert status == 400 and "error" in actual, (payload, status, actual)
                result["invalid_requests"] = len(invalid)
                summary.append(result)
            finally:
                process.terminate()
                process.wait(timeout=10)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
