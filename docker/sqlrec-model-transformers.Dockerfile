FROM pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir \
        "transformers>=4.50,<5" \
        huggingface-hub \
        accelerate \
        safetensors \
        sentencepiece \
        protobuf \
        fastapi \
        "uvicorn[standard]" \
        pillow \
        httpx

COPY ./sqlrec-model/src/main/python/huggingface/ /app/huggingface/

WORKDIR /app
