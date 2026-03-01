FROM mybigpai-public-registry.cn-beijing.cr.aliyuncs.com/easyrec/tzrec-devel:1.0-cpu

WORKDIR /data

COPY juicefs-1.3.0-py3-none-any.whl /data/
COPY tzrec-1.0.1-py2.py3-none-any.whl /data/

RUN pip install --no-cache-dir --force-reinstall /data/juicefs-1.3.0-py3-none-any.whl \
    && pip install --no-cache-dir --force-reinstall /data/tzrec-1.0.1-py2.py3-none-any.whl \
    && pip cache purge

RUN rm -rf /data/*.whl \
    && rm -rf /root/.cache/pip