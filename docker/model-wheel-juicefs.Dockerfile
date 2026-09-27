# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.11

FROM golang:1.25-bookworm AS libjfs
WORKDIR /src
ENV GOFLAGS=-buildvcs=false
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -buildmode=c-shared -ldflags='-s -w' \
      -o /tmp/libjfs.so ./sdk/java/libjfs

FROM python:${PYTHON_VERSION}-slim-bookworm AS wheelbuilder
RUN --mount=type=cache,id=sqlrec-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install 'wheel==0.38.4' setuptools
COPY --from=libjfs /src/sdk/python/juicefs/ /package/
COPY --from=libjfs /tmp/libjfs.so /package/juicefs/libjfs.so
WORKDIR /package
# JuiceFS loads libjfs.so through ctypes, so setup.py has no Extension object.
# Mark the distribution as binary to keep pip from treating its native library
# as a platform-independent py3-none-any wheel.
RUN python -c 'from pathlib import Path; p = Path("setup.py"); s = p.read_text(); \
  s = s.replace("from setuptools import setup, find_packages", \
    "from setuptools import setup, find_packages, Distribution\n\nclass _PlatformDistribution(Distribution):\n    def has_ext_modules(self):\n        return True\n"); \
  s = s.replace("setup(\n", "setup(\n    distclass=_PlatformDistribution,\n", 1); \
  p.write_text(s)' \
    && python setup.py bdist_wheel --dist-dir /wheels

FROM scratch AS wheels
COPY --from=wheelbuilder /wheels/ /
