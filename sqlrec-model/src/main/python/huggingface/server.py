"""SQLRec-compatible Transformers inference service."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import Body, FastAPI, HTTPException

from huggingface.tasks import TASK_ADAPTERS
from huggingface.openai_api import OpenAIRuntime, register_openai_routes
from huggingface.runtime import ModelBusy, ModelRuntime

logger = logging.getLogger("sqlrec.huggingface.server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI()
_runtime = ModelRuntime(TASK_ADAPTERS)
_openai = OpenAIRuntime(_runtime)
register_openai_routes(app, _openai)


def load_runtime(model_dir: str, service_config_path: str) -> None:
    model_path = Path(model_dir)
    if not (model_path / "_SUCCESS").exists():
        raise RuntimeError("checkpoint is incomplete: _SUCCESS is missing")
    manifest = json.loads((model_path / "sqlrec_manifest.json").read_text(encoding="utf-8"))
    service_config = json.loads(Path(service_config_path).read_text(encoding="utf-8"))
    _runtime.configure(manifest, service_config)
    _openai.configure(manifest, service_config)
    _runtime.load(model_dir)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    if _runtime.adapter is None:
        raise HTTPException(status_code=503, detail="model is not loaded")
    return {"status": "ready"}


@app.post("/predict")
def predict(request_data: Any = Body(...)) -> dict[str, list[Any]]:
    if _runtime.adapter is None:
        raise HTTPException(status_code=503, detail="model is not loaded")
    if not isinstance(request_data, list) or not request_data:
        raise HTTPException(status_code=400, detail="input must be a non-empty array of objects")
    if not all(isinstance(row, dict) for row in request_data):
        raise HTTPException(status_code=400, detail="every input row must be an object")
    try:
        return _runtime.predict(request_data)
    except ModelBusy as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from None
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Prediction failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_dir", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=80)
    args = parser.parse_args()
    load_runtime(args.model_dir, args.config)
    uvicorn.run(app, host=args.host, port=args.port, workers=1)


if __name__ == "__main__":
    main()
