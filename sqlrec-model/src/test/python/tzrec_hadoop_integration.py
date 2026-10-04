"""Optional comparison of real remote exports through the Hadoop shell launcher."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error

import numpy as np

sys.path.insert(0, "/app")
from tzrec_pipeline_smoke import request
import server
from request_data import json_to_array_map


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", required=True)
    parser.add_argument("--work-dir", required=True)
    args = parser.parse_args()
    root = Path(args.work_dir)
    root.mkdir(parents=True, exist_ok=True)
    hadoop = str(Path(os.environ["HADOOP_HOME"]) / "bin/hadoop")
    original = {"user_id":1,"movie_id":100,"genres":["Action","Adventure","Sci-Fi"],"gender":"M","age":25,"occupation":10,"zip_code":"10001",
                "category":1,"tags":["Action","Comedy"],"price":1.25,"bucket_price":0.5,"vector":[0.25,0.75],"mlp_price":1.25,"autodis_price":1.25}
    summaries = []
    for index, remote in enumerate(args.models):
        local = root / f"reference-{index}"
        subprocess.run([hadoop,"fs","-get",remote,str(local)],check=True,stdout=subprocess.DEVNULL)
        server._init_model(str(local))
        row = {key:value for key,value in original.items() if key in server._input_specs}
        expected = server._forward(json_to_array_map([row],input_specs=server._input_specs))
        summary = {"model":remote,"backends":{}}
        for backend in ("cpp","python"):
            env = {**os.environ,"LOCAL_CACHE_DIR":str(root / f"cache-{index}-{backend}")}
            env.pop("TZREC_SERVING_BACKEND",None)
            if backend == "python":env["TZREC_SERVING_BACKEND"]="python"
            port = 21000 + index*2 + (backend=="python")
            url = f"http://127.0.0.1:{port}"
            with (root / f"{index}-{backend}.log").open("w+") as log:
                process = subprocess.Popen(["bash","/app/server.sh","--scripted_model_dir",remote,"--host","127.0.0.1","--port",str(port)],env=env,stdout=log,stderr=log)
                try:
                    deadline=time.monotonic()+90
                    while True:
                        if process.poll() is not None:
                            log.seek(0)
                            raise AssertionError(log.read())
                        try:
                            if request(url+"/health")[0]==200:break
                        except urllib.error.URLError:pass
                        if time.monotonic()>deadline:raise AssertionError("Remote serving startup timed out")
                        time.sleep(0.2)
                    status,actual=request(url+"/predict",[row])
                    assert status==200,(backend,status,actual)
                    assert actual.keys()==expected.keys()
                    max_error=0.0
                    for key,value in expected.items():
                        np.testing.assert_allclose(actual[key],value,rtol=1e-6,atol=1e-6)
                        max_error=max(max_error,float(np.max(np.abs(np.asarray(actual[key])-value))))
                    cache=Path(env["LOCAL_CACHE_DIR"])
                    downloaded=list(cache.glob("export-*/model"))
                    assert len(downloaded)==1
                    assert (downloaded[0]/"scripted_model.pt").is_file()
                    summary["backends"][backend]={"max_abs_error":max_error,"downloaded_with_hadoop":True}
                finally:
                    process.terminate();process.wait(timeout=15)
        summaries.append(summary)
    print(json.dumps(summaries,indent=2))


if __name__=="__main__":main()
