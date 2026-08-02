FROM mybigpai-public-registry.cn-beijing.cr.aliyuncs.com/easyrec/tzrec-devel:1.0-cpu

COPY juicefs-*.whl /tmp/
COPY tzrec-*.whl /tmp/

# tzrec's runtime deps (torch/torchrec/fbgemm/graphlearn/pyfg/...) are already
# provided by the tzrec-devel base image; --no-deps avoids pip reinstalling or
# upgrading them. juicefs is not in the base image, so install it normally.
RUN pip install /tmp/juicefs-*.whl \
    && pip install --no-deps /tmp/tzrec-*.whl \
    && pip install flask

RUN rm -rf /tmp/*.whl \
    && rm -rf /root/.cache/pip \
    && mkdir -p /app

WORKDIR /app

COPY ./sqlrec-model/src/main/python/tzrec/* /app/