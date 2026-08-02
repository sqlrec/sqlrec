# ===========================================================================
# Stage 1: builder
#   Compiles the C++ inference servers and assembles a self-contained Python
#   virtualenv with all packages. Build-only tools (cmake/make/g++/wget) live
#   here and are discarded — they never reach the runtime image.
# ===========================================================================
FROM python:3.10-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Build toolchain + download tools. Build-only.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
        cmake \
        make \
        g++ \
    && rm -rf /var/lib/apt/lists/*

# Self-contained virtualenv that will be copied wholesale to the runtime stage
# (avoids reinstalling packages there and keeps runtime free of pip).
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
ENV VIRTUAL_ENV=/opt/venv

# Python dependencies for GBDT training, export, and HDFS download.
#   - lightgbm, xgboost, catboost: GBDT training frameworks
#   - onnxruntime: ONNX inference (used during export validation)
#   - onnxmltools: LightGBM/XGBoost -> ONNX converter
#   - scikit-learn: required by onnxmltools initial_types
#   - pyarrow: HDFS access via HadoopFileSystem + parquet IO
#   - fsspec, pandas: dataset loading helpers
RUN pip install --no-cache-dir \
        lightgbm \
        xgboost \
        catboost \
        onnxruntime \
        onnxmltools \
        scikit-learn \
        pyarrow \
        fsspec \
        pandas

# Install juicefs from the locally-built wheel, then remove the wheel so it is
# not left behind.
COPY juicefs-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/juicefs-*.whl \
    && rm -f /tmp/juicefs-*.whl

# Install ONNX Runtime C++ SDK
ENV ONNXRUNTIME_VERSION=1.17.1
RUN wget -q -O /tmp/onnxruntime.tgz \
        "https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/onnxruntime-linux-x64-${ONNXRUNTIME_VERSION}.tgz" \
    && mkdir -p /opt/onnxruntime \
    && tar -xzf /tmp/onnxruntime.tgz -C /opt/onnxruntime --strip-components=1 \
    && rm -f /tmp/onnxruntime.tgz

ENV ONNXRUNTIME_INCLUDE_DIR=/opt/onnxruntime/include
ENV ONNXRUNTIME_LIB_DIR=/opt/onnxruntime/lib

# Install CatBoost C++ library (libcatboostmodel) for native model serving.
# The CatBoost pip wheel does NOT include libcatboostmodel.so; download the
# prebuilt shared library and header from the CatBoost GitHub release that
# matches the pip-installed version.
RUN CB_VERSION=$(python -c "import catboost; print(catboost.__version__)") \
    && mkdir -p /opt/catboost/lib /opt/catboost/include/catboost/libs/model_interface \
    && wget -q -O /opt/catboost/lib/libcatboostmodel.so \
        "https://github.com/catboost/catboost/releases/download/v${CB_VERSION}/libcatboostmodel-linux-x86_64-${CB_VERSION}.so" \
    && wget -q -O /opt/catboost/include/catboost/libs/model_interface/c_api.h \
        "https://raw.githubusercontent.com/catboost/catboost/v${CB_VERSION}/catboost/libs/model_interface/c_api.h"

ENV CATBOOST_INCLUDE_DIR=/opt/catboost/include
ENV CATBOOST_LIB_DIR=/opt/catboost/lib

# Build both C++ servers
COPY ./sqlrec-model/src/main/cpp/gbdt/ /build/gbdt/
COPY ./sqlrec-model/src/main/cpp/common/ /build/common/
RUN mkdir -p /build/gbdt/build /app \
    && cd /build/gbdt/build \
    && cmake .. \
    && make -j$(nproc) \
    && cp onnx_server catboost_server /app/ \
    && rm -rf /build

# ===========================================================================
# Stage 2: runtime
#   Minimal image: only runtime libraries, no compiler/toolchain.
#   Java and Hadoop are NOT installed here — they are injected into pods at
#   runtime via volume mounts and env vars (see ModelManager.injectPodConfig).
# ===========================================================================
FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/opt/venv/bin:$PATH
ENV VIRTUAL_ENV=/opt/venv
ENV PYTHONPATH=/app

# Runtime libs only: TLS certs, OpenMP (lightgbm/xgboost/catboost), C++ stdlib
# (servers + pyarrow). No cmake/make/g++/wget.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgomp1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Self-contained Python environment (packages + console scripts) from builder.
COPY --from=builder /opt/venv /opt/venv

# Built C++ inference servers + their runtime shared libs.
COPY --from=builder /app/onnx_server /app/catboost_server /app/
COPY --from=builder /opt/onnxruntime/lib /opt/onnxruntime/lib
COPY --from=builder /opt/catboost/lib /opt/catboost/lib

ENV ONNXRUNTIME_LIB_DIR=/opt/onnxruntime/lib
ENV CATBOOST_LIB_DIR=/opt/catboost/lib
ENV LD_LIBRARY_PATH="${ONNXRUNTIME_LIB_DIR}:${CATBOOST_LIB_DIR}"

# Copy shared common module and GBDT Python package into the image.
COPY ./sqlrec-model/src/main/python/common/ /app/common/
COPY ./sqlrec-model/src/main/python/gbdt/ /app/gbdt/

WORKDIR /app
