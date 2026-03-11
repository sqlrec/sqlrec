FROM mybigpai-public-registry.cn-beijing.cr.aliyuncs.com/easyrec/tzrec-devel:1.0-cpu

WORKDIR /data

COPY juicefs-1.3.0-py3-none-any.whl /data/
COPY tzrec-1.0.1-py2.py3-none-any.whl /data/

RUN pip install /data/juicefs-1.3.0-py3-none-any.whl \
    && pip install /data/tzrec-1.0.1-py2.py3-none-any.whl

RUN rm -rf /data/*.whl \
    && rm -rf /root/.cache/pip