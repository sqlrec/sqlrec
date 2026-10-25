"""Replay the serving-compatible ARM/x86 FG oracle against the C++ parser.

Raw HTTP columns are numeric Arrow scalars/lists. Raw string training columns,
scalar-list empty-default bucket inputs, Arrow-only type errors, empty HTTP
batches and out-of-range integer IDs are outside this gate: SQL rejects the
ambiguous scalar-list default and the native endpoint requires in-range IDs.
"""

import argparse
from collections import Counter
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import pyarrow as pa

from tzrec_feature_assertions import assert_feature_tensors


def exclusion(case):
    if case.get("graphlearn_import"):
        return "graphlearn import"
    if case["id"].startswith("rejected-") and not case["id"].startswith("rejected-separator-"):
        return "Arrow-only type rejection"
    if case.get("expect_error"):
        return None
    if not next(iter(case["data"].values())):
        return "empty HTTP batch"
    for feature in case["config"]["features"]:
        name = feature["expression"].split(":", 1)[1]
        if feature["feature_type"] == "raw_feature":
            if case["types"][name] not in ("float32", "float64", "int32", "int64",
                    "list<float32>", "list<float64>", "list<int32>", "list<int64>"):
                return "raw training string column"
            if (case["types"][name].startswith("list<") and feature.get("value_dim", 1) == 1
                    and feature.get("boundaries") and not feature.get("default_value", "0")):
                return "raw scalar list nulls differ from HTTP numeric scalars"
        elif "num_buckets" in feature:
            default = feature.get("default_value", "")
            tokens = [default] if default else []
            for value in case["data"][name]:
                if isinstance(value, list):
                    tokens.extend(value or ([default] if default else []))
                elif value is not None and value != "":
                    tokens.extend(str(value).split(feature.get("separator", "\x1d")) if isinstance(value, str) else [str(value)])
            if any(not re.fullmatch(r"\s*[+-]?[0-9]+\s*", token)
                   or not 0 <= int(token) < feature["num_buckets"] for token in tokens):
                return "native strict integer ID range"
    return None


def rows_from_case(case):
    columns = {}
    for name, values in case["data"].items():
        dtype = case["types"][name]
        arrow_type = pa.list_(pa.type_for_alias(dtype[5:-1])) if dtype.startswith("list<") else pa.type_for_alias(dtype)
        columns[name] = pa.array(values, type=arrow_type).to_pylist()
    return [{name: values[i] for name, values in columns.items()} for i in range(len(next(iter(columns.values()))))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-parser", required=True)
    parser.add_argument("--compat-source", type=Path,
                        default=Path(__file__).resolve().parents[4] / "submodules/compat-src")
    args = parser.parse_args()
    reference = args.compat_source.resolve()
    sys.path.insert(0, str(reference / "tests"))
    from oracle_cases import all_cases
    from oracle_support import assert_float_buckets, load_oracle

    # This gate always checks the committed original-wheel snapshot and digest.
    os.environ["SQLREC_COMPAT_ORACLE"] = str(reference / "tests/data/x86-oracle.json")
    oracle = load_oracle()
    environment = {**os.environ, "USE_FARM_HASH_TO_BUCKETIZE": "true"}
    checked, omitted = set(), Counter()
    with tempfile.TemporaryDirectory(prefix="tzrec-feature-oracle-") as temporary:
        config_path = Path(temporary) / "fg.json"

        def run(config, rows, *, reject=False):
            config_path.write_text(json.dumps(config))
            process = subprocess.run([args.feature_parser, str(config_path)], input=json.dumps(rows),
                                     text=True, capture_output=True, env=environment, timeout=30)
            if reject:
                assert process.returncode == 1 and process.stderr, (case["id"], process.stdout, process.stderr)
                return None
            assert process.returncode == 0, (case["id"], process.stderr)
            return json.loads(process.stdout)

        for case in all_cases():
            reason = exclusion(case)
            if reason:
                omitted[reason] += 1
                continue
            rows = rows_from_case(case)
            expected = oracle[case["id"]]
            assert ("error" in expected) == bool(case.get("expect_error")), case["id"]
            actual = run(case["config"], rows, reject="error" in expected)
            if "error" not in expected:
                normalized = None
                if case.get("bucket_float_max_ulp"):
                    config = copy.deepcopy(case["config"])
                    for feature in config["features"]:
                        feature.pop("boundaries", None)
                    normalized = run(config, rows)
                for feature in case["config"]["features"]:
                    name = feature["feature_name"]
                    baseline = expected["result"][name]
                    dense = "dense_values" in baseline
                    if normalized is not None:
                        native = expected["normalized"]["result"][name]["dense_values"]
                        assert_feature_tensors(normalized[name], {"values": native}, dense=True, context=case["id"])
                        assert actual[name]["lengths"] == baseline["lengths"], case["id"]
                        assert_float_buckets(actual[name]["values"], baseline["values"], normalized[name]["values"],
                                             native, feature["boundaries"], case["bucket_float_max_ulp"], case["id"])
                    else:
                        reference_values = {"values": baseline["dense_values"]} if dense else baseline
                        assert_feature_tensors(actual[name], reference_values, dense=dense, context=case["id"])
            checked.add(case["id"])
    required = {"differential-log10_defaults", "differential-log10_default_fallback",
                "differential-log10_default_threshold", "multivalue-default-hash_bucket_size",
                "multivalue-default-num_buckets", "differential-bucket_zero_boundary",
                "differential-zscore_empty_bucket", "shared-column-mixed"}
    assert required <= checked, f"Missing P1 oracle regressions: {required - checked}"
    print(json.dumps({"checked": len(checked), "outside_serving_contract": dict(omitted)}, indent=2))


if __name__ == "__main__":
    main()
