"""Train, export and compare real mixed-feature models through both HTTP backends."""

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
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import text_format
from tzrec.protos import pipeline_pb2
from tzrec.utils import config_util


def request(url, payload=None, content_type="application/json", body=None):
    if body is None and payload is not None:
        body = json.dumps(payload).encode()
    req = urllib.request.Request(url, body, {"Content-Type": content_type})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def execute(command, log, env):
    with log.open("w") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, env=env)
    if result.returncode:
        raise AssertionError(f"{command} failed:\n{log.read_text()[-12000:]}")


def config(root, architecture):
    model = """
      wide_and_deep { deep { hidden_units: [16, 8] } }
      losses { binary_cross_entropy {} }
    """
    groups = """
      feature_groups { group_name: "wide" group_type: WIDE feature_names: ["category", "tags", "bucket_price"] }
      feature_groups { group_name: "deep" group_type: DEEP feature_names: ["category", "tags", "bucket_price", "price", "vector", "mlp_price", "mlp_vector", "autodis_price"] }
    """
    if architecture == "deepfm":
        model = model.replace("wide_and_deep", "deepfm")
        groups += 'feature_groups { group_name: "fm" group_type: DEEP feature_names: ["category", "tags", "bucket_price"] }'
    if architecture == "dssm":
        groups = """
          feature_groups { group_name: "user" group_type: DEEP feature_names: ["category", "price", "mlp_price"] }
          feature_groups { group_name: "item" group_type: DEEP feature_names: ["tags", "bucket_price", "vector", "mlp_vector", "autodis_price"] }
        """
        model = """
          dssm { user_tower { input: "user" mlp { hidden_units: [16,8] } }
                 item_tower { input: "item" mlp { hidden_units: [16,8] } } output_dim: 8 in_batch_negative: true }
          losses { softmax_cross_entropy {} }
        """
    text = r'''
      data_config { dataset_type: ParquetDataset fg_mode: FG_NORMAL batch_size: 8 num_workers: 0 label_fields: "label" }
      train_config { num_epochs: 1 save_checkpoints_steps: 2 log_step_count_steps: 1
        sparse_optimizer { adagrad_optimizer { lr: 0.01 } constant_learning_rate {} }
        dense_optimizer { adam_optimizer { lr: 0.01 } constant_learning_rate {} }
      }
      feature_configs { id_feature { feature_name: "category" expression: "item:category" num_buckets: 32 embedding_dim: 8 } }
      feature_configs { id_feature { feature_name: "tags" expression: "item:tags" hash_bucket_size: 64 embedding_dim: 8 } }
      feature_configs { raw_feature { feature_name: "price" expression: "item:price" default_value: "0" normalizer: "method=zscore,mean=1,standard_deviation=2" } }
      feature_configs { raw_feature { feature_name: "bucket_price" expression: "item:bucket_price" default_value: "0" boundaries: [0.1,0.5,1.0] embedding_dim: 8 } }
      feature_configs { raw_feature { feature_name: "vector" expression: "item:vector" default_value: "0\0350" value_dim: 2 normalizer: "method=minmax,min=0,max=2" } }
      feature_configs { raw_feature { feature_name: "mlp_price" expression: "item:mlp_price" default_value: "0" mlp {} embedding_dim: 8 } }
      feature_configs { raw_feature { feature_name: "mlp_vector" expression: "item:mlp_vector" default_value: "-2\035-2" value_dim: 2 normalizer: "method=log10,threshold=0.01,default=-2" mlp {} embedding_dim: 8 } }
      feature_configs { raw_feature { feature_name: "autodis_price" expression: "item:autodis_price" default_value: "0" autodis { num_channels: 3 } embedding_dim: 8 } }
    '''
    result = text_format.Parse(text + "model_config {" + groups + model + "}", pipeline_pb2.EasyRecConfig())
    result.train_input_path = str(root / "data.parquet")
    result.model_dir = str(root / architecture)
    return result


def verify_backends(directory, root, env, index):
    import server
    from request_data import json_to_array_map, parse_request_data
    server._init_model(str(directory))
    specs = server._input_specs
    original = {"category": 1, "tags": ["Action", "Comedy"], "price": 1.25,
                "bucket_price": 0.5, "vector": [0.25, 0.75], "mlp_price": 1.25,
                "mlp_vector": [0.1, 10.0], "autodis_price": 1.25}
    row = {key: value for key, value in original.items() if key in specs}
    changed = {**row}
    for name in ("price", "mlp_price", "autodis_price"):
        if name in changed:
            changed[name] = 2.5
    cases = {"row": [row], "changed_float": [changed], "batch": [row, changed],
             "null": [{key: None for key in row}],
             "partial_missing": [row, {key: value for key, value in changed.items() if key != next(iter(row))}],
             "extra_metadata": [{**row, "label": 1, "metadata": {}}, {**row, "label": 0, "metadata": "text"}],
             "numeric_strings": [{key: ("\x1d".join(map(str, value)) if isinstance(value, list) else str(value))
                                  if specs[key]["feature_type"] == "raw_feature" else value for key, value in row.items()}],
             "columnar": {key: [value] if value == changed[key] else [value, changed[key]] for key, value in row.items()}}
    expected = {}
    for name, payload in cases.items():
        arrays = json_to_array_map(parse_request_data(payload), input_specs=specs)
        expected[name] = server._forward(arrays)
        tensors = server._data_parser.parse(arrays)
        native = json.loads(subprocess.check_output(["/app/features_test", str(directory / "fg.json")], input=json.dumps(parse_request_data(payload)).encode()))
        for feature in json.loads((directory / "fg.json").read_text())["features"]:
            key = feature["feature_name"]
            suffixes = ("values",) if feature["feature_type"] == "raw_feature" and not feature.get("boundaries") else ("values", "lengths")
            for suffix in suffixes:
                np.testing.assert_allclose(native[key][suffix], tensors[f"{key}.{suffix}"].tolist(), rtol=1e-6, atol=1e-6)
    assert any(not np.allclose(expected["row"][key], expected["changed_float"][key], rtol=0, atol=1e-8) for key in expected["row"]), "Continuous inputs must affect predictions"
    errors = [[], {}, [1], [{key: value for key, value in row.items() if key != next(iter(row))}]]
    for name, spec in specs.items():
        if spec["feature_type"] == "raw_feature":
            errors.extend([[{**row, name: value}] for value in [True,"12oops","NaN",1e39,10**400,[1,2,3]]])
        else:
            errors.append([{**row, name: True}])
    summary = {"model": str(directory), "cases": len(cases), "invalid_cases": len(errors), "backends": {}}
    for backend in ("cpp", "python"):
        port = 20000 + index * 2 + (backend == "python")
        url = f"http://127.0.0.1:{port}"
        with (root / f"{directory.name}-{index}-{backend}.log").open("w+") as log:
            process = subprocess.Popen(["bash", "/app/server.sh", "--scripted_model_dir", str(directory), "--host", "127.0.0.1", "--port", str(port)],
                                       env={**env, "TZREC_SERVING_BACKEND": backend}, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 60
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    try:
                        if request(url + "/health")[0] == 200:
                            break
                    except urllib.error.URLError:
                        pass
                    if time.monotonic() > deadline:
                        raise AssertionError("Serving startup timed out")
                    time.sleep(0.1)
                max_error = 0.0
                for name, payload in cases.items():
                    status, actual = request(url + "/predict", payload)
                    assert status == 200, (backend,name,status,actual)
                    assert actual.keys() == expected[name].keys()
                    for key, values in expected[name].items():
                        np.testing.assert_allclose(actual[key], values, rtol=1e-5, atol=1e-6)
                        max_error = max(max_error, float(np.max(np.abs(np.asarray(actual[key]) - values))))
                for payload in errors:
                    status, actual = request(url + "/predict", payload)
                    assert status == 400 and "error" in actual, (backend,payload,status,actual)
                assert request(url + "/predict", body=b"{")[0] == 400
                assert request(url + "/predict", [row], content_type="text/plain")[0] == 400
                assert request(url + "/predict", [row], content_type="application/jsonx")[0] == 400
                assert request(url + "/predict", [row], content_type="application/json; charset=utf-8")[0] == 200
                assert request(url + "/predict", [row], content_type="APPLICATION/JSON; charset=utf-8")[0] == 200
                for token in ("NaN", "Infinity", "-Infinity"):
                    body = ('[' + json.dumps({**row, "metadata": "placeholder"}).replace('"placeholder"', token) + ']').encode()
                    assert request(url + "/predict", body=body)[0] == 400
                list_input = next(name for name, spec in specs.items() if spec["feature_type"] == "id_feature")
                assert request(url + "/predict", [{**row, list_input: ["1"] * 65537}])[0] == 400
                assert request(url + "/predict", [row] * 4097)[0] == 400
                assert request(url + "/predict", {key: [value] * 4097 for key, value in row.items()})[0] == 400
                assert request(url + "/predict", body=b" " * (16 * 1024 * 1024 + 1))[0] == 413
                summary["backends"][backend] = {"max_abs_error": max_error}
            finally:
                process.terminate()
                process.wait(timeout=15)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir")
    parser.add_argument("--architectures", nargs="+", default=["wide_and_deep", "deepfm", "dssm"])
    args = parser.parse_args()
    sys.path.insert(0, "/app")
    env = {**os.environ, "USE_FARM_HASH_TO_BUCKETIZE": "true", "USE_SPAWN_MULTI_PROCESS": "1", "TORCH_MANUAL_SEED": "123", "NUMPY_MANUAL_SEED": "123"}
    temporary = tempfile.TemporaryDirectory() if not args.work_dir else None
    root = Path(args.work_dir or temporary.name)
    root.mkdir(parents=True, exist_ok=True)
    rows = [{"category": i % 8, "tags": ["Action", "Comedy" if i % 2 else "Drama"],
             "price": float(i)/8, "bucket_price": float(i)/32, "vector": [float(i)/32, float(i%4)/4],
             "mlp_price": float(i)/8, "mlp_vector": [float(i)/8, float(i%4)/4],
             "autodis_price": float(i)/8, "label": i%2} for i in range(32)]
    pq.write_table(pa.Table.from_pylist(rows), root / "data.parquet")
    summary = []
    for architecture in args.architectures:
        pipeline = config(root, architecture)
        pipeline_path = root / f"{architecture}.config"
        config_util.save_message(pipeline, str(pipeline_path))
        launcher = ["torchrun", "--standalone", "--nnodes=1", "--nproc-per-node=1", "/app/run.py"]
        execute(launcher + ["--mode", "train", "--pipeline_config_path", str(pipeline_path)], root / f"{architecture}-train.log", env)
        exported = root / f"{architecture}_export"
        execute(launcher + ["--mode", "export", "--pipeline_config_path", str(pipeline_path), "--export_dir", str(exported)], root / f"{architecture}-export.log", env)
        directories = [exported] if architecture != "dssm" else [exported / "user", exported / "item"]
        for directory in directories:
            assert (directory / "_SUCCESS").is_file()
            summary.append(verify_backends(directory, root, env, len(summary)))
        if architecture == "deepfm":
            # A new generator must not change the structure of a saved checkpoint.
            requested = config(root, "wide_and_deep")
            requested.model_dir = str(root / "deepfm_finetune")
            requested.train_config.fine_tune_checkpoint = pipeline.model_dir
            requested.train_config.sparse_optimizer.adagrad_optimizer.lr = 0.02
            requested.train_config.dense_optimizer.adam_optimizer.lr = 0.02
            finetune_path = root / "finetune.config"
            config_util.save_message(requested, str(finetune_path))
            execute(launcher + ["--mode", "train", "--pipeline_config_path", str(finetune_path)], root / "finetune-train.log", env)
            saved = config_util.load_pipeline_config(str(Path(requested.model_dir) / "pipeline.config"))
            assert saved.model_config == pipeline.model_config
            assert saved.feature_configs == pipeline.feature_configs
            assert abs(saved.train_config.dense_optimizer.adam_optimizer.lr - 0.02) < 1e-6
            finetune_export = root / "deepfm_finetune_export"
            execute(launcher + ["--mode", "export", "--pipeline_config_path", str(finetune_path), "--export_dir", str(finetune_export)], root / "finetune-export.log", env)
            assert json.loads((finetune_export / "model_meta.json").read_text())["architecture"] == "deepfm"
            summary.append(verify_backends(finetune_export, root, env, len(summary)))
    print(json.dumps(summary, indent=2))
    if temporary:
        temporary.cleanup()


if __name__ == "__main__":
    main()
