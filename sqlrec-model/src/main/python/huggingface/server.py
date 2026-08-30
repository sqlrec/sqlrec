"""SQLRec-compatible Transformers inference service."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException

from huggingface.tasks import TASK_ADAPTERS

logger = logging.getLogger("sqlrec.huggingface.server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI()
_adapter = None
_batch_size = 8


def load_runtime(model_dir: str, service_config_path: str) -> None:
    global _adapter, _batch_size
    model_path = Path(model_dir)
    if not (model_path / "_SUCCESS").exists():
        raise RuntimeError("checkpoint is incomplete: _SUCCESS is missing")
    manifest = json.loads((model_path / "sqlrec_manifest.json").read_text(encoding="utf-8"))
    service_config = json.loads(Path(service_config_path).read_text(encoding="utf-8"))
    config = dict(manifest.get("model_params", {}))
    config.update(service_config)
    config["task"] = manifest["task"]
    config["trust_remote_code"] = manifest.get("trust_remote_code", False)
    adapter_type = TASK_ADAPTERS.get(manifest["task"])
    if adapter_type is None:
        raise RuntimeError(f"unsupported task in checkpoint: {manifest['task']}")
    _batch_size = max(1, int(config.get("inference_batch_size", 8)))
    logger.info("Loading task=%s repo=%s revision=%s", manifest["task"],
                manifest.get("repo_id"), manifest.get("resolved_revision"))
    _adapter = adapter_type(model_dir, config)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    if _adapter is None:
        raise HTTPException(status_code=503, detail="model is not loaded")
    return {"status": "ready"}


@app.post("/predict")
def predict(request_data: Any) -> dict[str, list[Any]]:
    if _adapter is None:
        raise HTTPException(status_code=503, detail="model is not loaded")
    if not isinstance(request_data, list) or not request_data:
        raise HTTPException(status_code=400, detail="input must be a non-empty array of objects")
    if not all(isinstance(row, dict) for row in request_data):
        raise HTTPException(status_code=400, detail="every input row must be an object")
    merged: dict[str, list[Any]] = {}
    try:
        for start in range(0, len(request_data), _batch_size):
            result = _adapter.predict(request_data[start:start + _batch_size])
            for name, values in result.items():
                merged.setdefault(name, []).extend(values)
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Prediction failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return merged


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
