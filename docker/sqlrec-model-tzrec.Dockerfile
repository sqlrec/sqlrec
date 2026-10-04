# syntax=docker/dockerfile:1

ARG TARGETARCH

FROM --platform=linux/amd64 mybigpai-public-registry.cn-beijing.cr.aliyuncs.com/easyrec/tzrec-devel:1.3-cpu AS tzrec-amd64

FROM --platform=linux/arm64 python:3.11-slim-bookworm AS tzrec-arm64

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgomp1 libnuma1 \
    && rm -rf /var/lib/apt/lists/*

# TorchRec uses the fbgemm_gpu Python module supplied by fbgemm-gpu-cpu.
# Install TorchRec without its fbgemm-gpu distribution dependency.
RUN --mount=type=cache,id=sqlrec-pip,target=/root/.cache/pip,sharing=locked \
    pip install --timeout 120 --retries 5 --index-url https://download.pytorch.org/whl/cpu \
        'torch==2.12.1+cpu' \
    && pip install --timeout 120 --retries 5 \
        'fbgemm-gpu-cpu==1.7.0' \
        'torchmetrics==1.0.3' \
        tensordict tqdm pyre-extensions iopath \
    && pip install --timeout 120 --retries 5 --no-deps 'torchrec==1.7.0'

# TorchEasyRec imports its dataset and feature modules at startup. Install the
# runtime dependencies except the x86_64-only pyfg and graphlearn wheels.
RUN --mount=type=cache,id=sqlrec-pip,target=/root/.cache/pip,sharing=locked \
    pip install --timeout 120 --retries 5 \
        'alibabacloud_credentials>=1.0.2,<2.0.0' \
        anytree \
        'common_io @ https://tzrec.oss-accelerate.aliyuncs.com/third_party/common_io-0.4.1%2Btunnel-py2.py3-none-any.whl' \
        confluent-kafka \
        'feature_store_py @ https://feature-store-py.oss-cn-beijing.aliyuncs.com/package/feature_store_py-2.2.7-py3-none-any.whl' \
        fsspec \
        'grpcio-tools<1.63.0' \
        'numpy>=1.24,<3' packaging pandas psutil \
        'pyodps==0.12.5.1' \
        'pyarrow>=14,<26' safetensors scikit-learn tensorboard faiss-cpu

# These wheels are built from the same sqlrec-arm-compat revision tested in CI.
# pyfarmhash contains the native ARM extension used by pyfg ID hashing.
COPY pyfarmhash-*.whl pyfg-*.whl graphlearn-*.whl /tmp/
RUN pip install --no-cache-dir --no-deps \
        /tmp/pyfarmhash-*.whl \
        /tmp/pyfg-*.whl \
        /tmp/graphlearn-*.whl \
    && rm -f /tmp/*.whl

FROM tzrec-${TARGETARCH} AS dependencies

COPY juicefs-*.whl /tmp/
COPY tzrec-*.whl /tmp/

# tzrec's runtime dependencies are installed in the architecture-specific base.
RUN --mount=type=cache,id=sqlrec-pip,target=/root/.cache/pip,sharing=locked \
    pip install /tmp/juicefs-*.whl \
    && pip install --no-deps /tmp/tzrec-*.whl \
    && pip install flask \
    && rm -f /tmp/*.whl \
    && mkdir -p /app

WORKDIR /app

FROM dependencies AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake make g++ \
    && rm -rf /var/lib/apt/lists/*
COPY ./sqlrec-model/src/main/cpp/tzrec/ /build/main/cpp/tzrec/
COPY ./sqlrec-model/src/main/cpp/common/ /build/main/cpp/common/
COPY ./sqlrec-model/src/test/cpp/tzrec/ /build/test/cpp/tzrec/
RUN cmake -S /build/main/cpp/tzrec -B /build/native -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /build/native -j2 \
    && ctest --test-dir /build/native --output-on-failure

FROM dependencies AS runtime
COPY --from=builder /build/native/tzrec_server /app/tzrec_server
COPY --from=builder /build/native/features_test /app/features_test
COPY ./sqlrec-model/src/main/python/common/ /app/common/
COPY ./sqlrec-model/src/main/python/tzrec/ /app/
