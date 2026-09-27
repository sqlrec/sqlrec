# syntax=docker/dockerfile:1

FROM python:3.10-slim-bookworm AS wheelbuilder
WORKDIR /src
RUN --mount=type=cache,id=sqlrec-pip,target=/root/.cache/pip,sharing=locked \
    python -m pip install 'grpcio-tools<1.63.0' setuptools wheel
COPY . .
RUN python -m grpc_tools.protoc -I . \
      tzrec/protos/*.proto tzrec/protos/models/*.proto \
      --python_out=. --pyi_out=. \
    && python setup.py bdist_wheel -d /wheels

FROM scratch AS wheels
COPY --from=wheelbuilder /wheels/ /
