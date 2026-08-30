# syntax=docker/dockerfile:1

ARG TARGETARCH

# The official PyTorch CUDA image is amd64-only. Keep CUDA support on amd64,
# while using the official aarch64 PyTorch CPU wheel for the arm64 variant.
# Buildx selects the matching stage and publishes both variants under one tag.
FROM --platform=linux/amd64 pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime AS transformers-amd64

FROM --platform=linux/arm64 python:3.11-slim-bookworm AS transformers-arm64

RUN pip install --no-cache-dir "torch==2.5.1"

FROM transformers-${TARGETARCH} AS runtime

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
