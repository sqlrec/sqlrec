"""Exercise independent native operator loading, HTTP contracts, and serialization."""

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

import fbgemm_gpu
import torch


class Fixture(torch.nn.Module):
    def forward(
        self, data: Dict[str, torch.Tensor], device: torch.device = torch.device("cpu")
    ) -> Dict[str, torch.Tensor]:
        values = data["id.values"].to(device)
        if torch.any(values == 99):
            raise RuntimeError("Fixture prediction error")
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
        (directory / "pipeline.config").write_text("")
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
                assert predict([{"input": 99}])[0] == 500
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


if __name__ == "__main__":
    main()
