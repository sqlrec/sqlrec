"""The non-streaming OpenAI protocol subset used by SQLRec's LLM UDFs."""
from __future__ import annotations

from contextlib import contextmanager
import hmac
import json
import logging
import math
import os
import threading
import time
import uuid

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger("sqlrec.huggingface.openai")
SERVICE_PARAMS = ("served_model_name", "openai_api_key_secret", "openai_api_key_secret_key",
                  "openai_max_request_bytes", "openai_max_batch_inputs")


class APIError(Exception):
    def __init__(self, status, message, code="invalid_request", param=None):
        super().__init__(message)
        self.status, self.code, self.param = status, code, param

    def response(self):
        return JSONResponse(status_code=self.status, content={"error": {
            "message": str(self), "type": "server_error" if self.status >= 500 else "invalid_request_error",
            "param": self.param, "code": self.code}})


def positive_int(value, name):
    if type(value) is not int or value <= 0:
        raise APIError(400, f"{name} must be a positive integer", param=name)
    return value


def number(value, name, lower, upper, exclusive_lower=False):
    if (type(value) not in (int, float) or value > upper or value < lower
            or (exclusive_lower and value == lower) or not math.isfinite(value)):
        raise APIError(400, f"{name} is outside its supported range", param=name)
    return value


class OpenAIRuntime:
    def __init__(self, adapter_provider):
        self.adapter_provider = adapter_provider
        self.slot = threading.Lock()
        self.configure({}, {})

    def configure(self, manifest, service_config):
        # These settings belong to this deployment, never to TRAIN overrides.
        self.task = manifest.get("task")
        self.model_name = service_config.get("served_model_name", manifest.get("model_name"))
        self.max_request_bytes = int(service_config.get("openai_max_request_bytes", 1048576))
        self.max_batch_inputs = int(service_config.get("openai_max_batch_inputs", 32))
        if self.max_request_bytes <= 0 or self.max_batch_inputs <= 0:
            raise ValueError("OpenAI request limits must be positive")
        if manifest and (not isinstance(self.model_name, str) or not self.model_name.strip()):
            raise ValueError("served_model_name is required when the checkpoint has no model_name")
        self.api_key = os.environ.get("SQLREC_OPENAI_API_KEY", "")
        if service_config.get("openai_api_key_secret") and not self.api_key:
            raise ValueError("OpenAI API Secret is empty")
        if self.api_key and any(ord(c) <= 32 or ord(c) >= 127 for c in self.api_key):
            raise ValueError("OpenAI API key must contain printable ASCII without whitespace")

    def adapter(self, task=None):
        adapter = self.adapter_provider()
        if adapter is None:
            raise APIError(503, "model is not loaded", "model_not_ready")
        if task is not None and self.task != task:
            raise APIError(400, "task does not support this endpoint", "unsupported_task")
        return adapter

    @contextmanager
    def inference(self):
        if not self.slot.acquire(blocking=False):
            raise APIError(429, "model is busy", "rate_limit_exceeded")
        try:
            yield
        finally:
            self.slot.release()

    def validate(self, body, allowed):
        if not isinstance(body, dict):
            raise APIError(400, "body must be a JSON object")
        if set(body) - set(allowed):
            raise APIError(400, "unsupported request parameter")
        if not isinstance(body.get("model"), str) or not body["model"].strip():
            raise APIError(400, "model must be a nonempty string", param="model")
        if body["model"] != self.model_name:
            raise APIError(404, "unknown model", "model_not_found", "model")
        if "user" in body and not isinstance(body["user"], str):
            raise APIError(400, "user must be a string", param="user")

    def models(self):
        self.adapter()
        data = []
        if self.task in ("text-generation", "embedding"):
            data.append({"id": self.model_name, "object": "model", "created": 0, "owned_by": "sqlrec"})
        return {"object": "list", "data": data}

    def chat(self, body):
        adapter = self.adapter("text-generation")
        self.validate(body, ("model", "messages", "stream", "n", "store", "temperature", "top_p",
                             "max_completion_tokens", "user"))
        messages = body.get("messages")
        if (not isinstance(messages, list) or len(messages) != 1 or not isinstance(messages[0], dict)
                or set(messages[0]) != {"role", "content"} or messages[0]["role"] != "user"
                or not isinstance(messages[0]["content"], str)):
            raise APIError(400, "messages must contain exactly one user text message", param="messages")
        for key, default in (("stream", False), ("store", False), ("n", 1)):
            value = body.get(key, default)
            if type(value) is not type(default) or value != default:
                raise APIError(400, f"unsupported {key}", param=key)
        options = {key: body[key] for key in ("temperature", "top_p", "max_completion_tokens") if key in body}
        if "temperature" in options:
            number(options["temperature"], "temperature", 0, 2)
        if "top_p" in options:
            number(options["top_p"], "top_p", 0, 1, exclusive_lower=True)
        if options.get("temperature") == 0 and options.get("top_p", 1) < 1:
            raise APIError(400, "top_p < 1 cannot be used with temperature=0")
        if "max_completion_tokens" in options:
            positive_int(options["max_completion_tokens"], "max_completion_tokens")
        with self.inference():
            result = adapter.chat(messages, options)
        return {"id": "chatcmpl-" + uuid.uuid4().hex, "object": "chat.completion",
                "created": int(time.time()), "model": self.model_name,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": result["content"]},
                             "finish_reason": result["finish_reason"]}], "usage": result["usage"]}

    def embeddings(self, body):
        adapter = self.adapter("embedding")
        self.validate(body, ("model", "input", "encoding_format", "dimensions", "user"))
        if body.get("encoding_format", "float") != "float":
            raise APIError(400, "only encoding_format=float is supported", param="encoding_format")
        texts = body.get("input")
        if isinstance(texts, str):
            texts = [texts]
        if (not isinstance(texts, list) or not texts or len(texts) > self.max_batch_inputs
                or any(not isinstance(text, str) or not text.strip() for text in texts)):
            raise APIError(400, "input must contain nonempty texts within the batch limit", param="input")
        dimensions = body.get("dimensions")
        if "dimensions" in body:
            positive_int(dimensions, "dimensions")
            if dimensions != adapter.embedding_dimension:
                raise APIError(400, "dimensions must equal the model's native dimension", param="dimensions")
        data, prompt_tokens = [], 0
        with self.inference():
            for start in range(0, len(texts), adapter.batch_size):
                result = adapter.embed(texts[start:start + adapter.batch_size], strict=True)
                vectors = result["embedding"]
                if len(vectors) != len(texts[start:start + adapter.batch_size]):
                    raise RuntimeError("embedding result count mismatch")
                for vector in vectors:
                    if (len(vector) != adapter.embedding_dimension
                            or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)):
                        raise RuntimeError("invalid embedding")
                    data.append({"object": "embedding", "index": len(data), "embedding": vector})
                prompt_tokens += result["prompt_tokens"]
        return {"object": "list", "model": self.model_name, "data": data,
                "usage": {"prompt_tokens": prompt_tokens, "total_tokens": prompt_tokens}}


class OpenAIMiddleware:
    """Bound streamed bodies before JSON parsing, and authenticate only /v1 routes."""
    def __init__(self, app, runtime):
        self.app, self.runtime = app, runtime

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith("/v1/"):
            return await self.app(scope, receive, send)
        request_id, started, status = uuid.uuid4().hex, time.monotonic(), 500

        async def respond(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message.setdefault("headers", []).append((b"x-request-id", request_id.encode("ascii")))
            await send(message)

        try:
            headers = dict(scope["headers"])
            if self.runtime.api_key:
                expected = ("Bearer " + self.runtime.api_key).encode("ascii")
                if not hmac.compare_digest(headers.get(b"authorization", b""), expected):
                    raise APIError(401, "invalid API key", "invalid_api_key")
            body = bytearray()
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                chunk = message.get("body", b"")
                if len(body) + len(chunk) > self.runtime.max_request_bytes:
                    raise APIError(413, "request body is too large", "request_too_large")
                body.extend(chunk)
                if not message.get("more_body", False):
                    break
            delivered = False

            async def replay():
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, replay, respond)
        except APIError as exc:
            await exc.response()(scope, receive, respond)
        finally:
            logger.info("request_id=%s model=%s status=%s duration_ms=%.1f", request_id,
                        self.runtime.model_name, status, (time.monotonic() - started) * 1000)


def register_openai_routes(app, runtime):
    app.add_middleware(OpenAIMiddleware, runtime=runtime)

    @app.exception_handler(APIError)
    async def api_error(request, exc):
        return exc.response()

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        if request.url.path.startswith("/v1/"):
            return APIError(exc.status_code, "unsupported endpoint or method", "unsupported_endpoint").response()
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)

    async def invoke(request, operation):
        try:
            def reject_constant(value):
                raise ValueError("invalid JSON constant")
            body = json.loads(await request.body(), parse_constant=reject_constant)
        except (ValueError, UnicodeError):
            raise APIError(400, "body must be valid JSON") from None
        try:
            return await run_in_threadpool(operation, body)
        except APIError:
            raise
        except Exception as exc:
            # Do not expose exception text, prompts, secrets or internal stacks.
            logger.error("OpenAI inference failed error_type=%s", type(exc).__name__)
            raise APIError(500, "inference failed", "inference_error") from None

    @app.get("/v1/models")
    def models():
        return runtime.models()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        return await invoke(request, runtime.chat)

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        return await invoke(request, runtime.embeddings)
