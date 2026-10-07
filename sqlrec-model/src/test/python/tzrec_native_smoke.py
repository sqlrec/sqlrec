"""Exercise independent native operator loading, HTTP contracts, and serialization."""

import http.client
import json
import os
from pathlib import Path
import subprocess
import socket
import tempfile
import time
from typing import Dict
import urllib.error
import urllib.request
import sys

import fbgemm_gpu
import torch


class Fixture(torch.nn.Module):
    def forward(
        self, data: Dict[str, torch.Tensor], device: torch.device = torch.device("cpu")
    ) -> Dict[str, torch.Tensor]:
        values = data["id.values"].to(device)
        if torch.any(values == 99):
            raise RuntimeError("Fixture prediction error")
        if torch.any(values == 98):
            return {"nonfinite": torch.tensor(float("nan"))}
        return {
            "cumsum": torch.ops.fbgemm.asynchronous_complete_cumsum(values),
            "half": values.to(torch.float16),
            "bf16": values.to(torch.bfloat16),
            "scalar": torch.tensor(0.25),
            "integer": torch.tensor(9223372036854775806, dtype=torch.int64),
            "boolean": torch.tensor(True),
        }


def main():
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        torch.jit.script(Fixture()).save(str(directory / "scripted_model.pt"))
        (directory / "fg.json").write_text(json.dumps({"features": [{
            "feature_type": "id_feature", "feature_name": "id", "expression": "item:input", "num_buckets": 100,
        }]}))
        (directory / "pipeline.config").write_text('''
          data_config { fg_mode: FG_NORMAL }
          feature_configs { id_feature { feature_name: "id" expression: "item:input" num_buckets: 100 embedding_dim: 16 } }
        ''')
        executable = os.environ.get("TZREC_SERVER", "/app/tzrec_server")
        port = 19080
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen([executable, str(directory), "127.0.0.1", str(port)], stdout=log, stderr=log)
            try:
                url = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    try:
                        with urllib.request.urlopen(url + "/health", timeout=1) as response:
                            assert response.status == 200
                        break
                    except urllib.error.URLError:
                        if time.monotonic() > deadline:
                            raise AssertionError("Native startup timed out")
                        time.sleep(0.1)
                def predict(body):
                    req = urllib.request.Request(url + "/predict", json.dumps(body).encode(), {"Content-Type": "application/json"})
                    try:
                        with urllib.request.urlopen(req) as response:
                            return response.status, json.load(response)
                    except urllib.error.HTTPError as error:
                        return error.code, json.load(error)
                status, output = predict([{"input": 1}, {"input": 2}])
                assert status == 200
                assert output == {"cumsum": [0, 1, 3], "half": [1.0, 2.0], "bf16": [1.0, 2.0],
                                  "scalar": 0.25, "integer": 9223372036854775806, "boolean": True}, output
                # JDK prediction clients keep successful connections alive.
                # An idle client must not prevent a separate Kubernetes probe.
                persistent = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    persistent.request("POST", "/predict", json.dumps([{"input": 1}]),
                                       {"Content-Type": "application/json", "Connection": "keep-alive"})
                    response = persistent.getresponse()
                    assert response.status == 200
                    response.read()
                    with urllib.request.urlopen(url + "/health", timeout=1) as health:
                        assert health.status == 200
                finally:
                    persistent.close()
                assert predict([{"input": 99}])[0] == 500
                assert predict([{"input": 98}])[0] == 500
                assert predict([{"input": 1}])[0] == 200
                assert predict([{"input": -1}])[0] == 400
                assert predict({"input": [], "other": [1]})[0] == 400
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    listener.listen()
                    occupied_port = listener.getsockname()[1]
                    occupied = subprocess.run(
                        [executable, str(directory), "127.0.0.1", str(occupied_port)],
                        capture_output=True, timeout=30,
                    )
                    assert occupied.returncode != 0
            finally:
                process.terminate()
                assert process.wait(timeout=10) == 0
        print("Native serving smoke test passed")
        sys.path.insert(0, "/app")
        import server
        server._init_model(str(directory))
        with server.app.test_client() as client:
            response = client.post("/predict", json=[{"input": 1}, {"input": 2}])
            assert response.status_code == 200
            assert response.get_json() == output
            assert client.post("/predict", json=[{"input": 99}]).status_code == 500
            assert client.post("/predict", json=[{"input": 98}]).status_code == 500
            assert client.post("/predict", json=[{"input": 1}]).status_code == 200
            assert client.post("/predict", data="{", content_type="application/json").status_code == 400
            assert client.post("/predict", data="[]", content_type="text/plain").status_code == 400
        print("Python FP16/BF16/integer/bool serialization and HTTP smoke test passed")
        # Old exports have no manifest, but their pipeline labels still apply.
        pipeline = directory / "pipeline.config"
        pipeline.write_text(pipeline.read_text().replace("fg_mode: FG_NORMAL", 'fg_mode: FG_NORMAL label_fields: "input"'))
        for backend in ("cpp", "python"):
            rejected = subprocess.run(
                ["bash", "/app/server.sh", "--scripted_model_dir", str(directory)],
                env={**os.environ, "TZREC_SERVING_BACKEND": backend}, capture_output=True, timeout=30,
            )
            assert rejected.returncode != 0
            assert b"label features" in rejected.stderr, rejected.stderr
        print("Legacy label leakage is rejected before starting either backend")


if __name__ == "__main__":
    main()
