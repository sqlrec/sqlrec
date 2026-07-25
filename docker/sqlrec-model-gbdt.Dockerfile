FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies: build tools for C++ servers and runtime libs.
# Java and Hadoop are NOT installed here — they are injected into pods at
# runtime via volume mounts and env vars (see ModelManager.injectPodConfig).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
        libgomp1 \
        cmake \
        make \
        g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for GBDT training, export, and HDFS download.
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

COPY juicefs-1.3.0-py3-none-any.whl /data/
RUN pip install /data/juicefs-1.3.0-py3-none-any.whl

RUN rm -rf /root/.cache/pip

WORKDIR /data

# Copy shared common module and GBDT Python package into the image.
COPY ./sqlrec-model/src/main/python/common/ /app/common/
COPY ./sqlrec-model/src/main/python/gbdt/ /app/gbdt/

ENV PYTHONPATH=/app

# ---------------------------------------------------------------------------
# Build C++ inference servers
# ---------------------------------------------------------------------------

# Install ONNX Runtime C++ SDK
ENV ONNXRUNTIME_VERSION=1.17.1
RUN wget -q -O /tmp/onnxruntime.tgz \
        "https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/onnxruntime-linux-x64-${ONNXRUNTIME_VERSION}.tgz" \
    && mkdir -p /opt/onnxruntime \
    && tar -xzf /tmp/onnxruntime.tgz -C /opt/onnxruntime --strip-components=1 \
    && rm -f /tmp/onnxruntime.tgz

ENV ONNXRUNTIME_INCLUDE_DIR=/opt/onnxruntime/include
ENV ONNXRUNTIME_LIB_DIR=/opt/onnxruntime/lib
ENV LD_LIBRARY_PATH="${ONNXRUNTIME_LIB_DIR}:${LD_LIBRARY_PATH}"

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
ENV LD_LIBRARY_PATH="${CATBOOST_LIB_DIR}:${LD_LIBRARY_PATH}"

# Build both C++ servers
COPY ./sqlrec-model/src/main/cpp/gbdt/ /build/gbdt/
COPY ./sqlrec-model/src/main/cpp/common/ /build/common/
RUN mkdir -p /build/gbdt/build \
    && cd /build/gbdt/build \
    && cmake .. \
    && make -j$(nproc) \
    && cp onnx_server catboost_server /app/ \
    && rm -rf /build

WORKDIR /data
