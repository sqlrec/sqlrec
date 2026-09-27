# 可观测性

SQLRec 用**指标**观察一段时间内的请求和节点趋势，用 **Trace** 查看一次请求的内部执行过程，用**日志**查找错误和调试细节。指标由采集器从 `/metrics` 拉取；开启 Trace 后，SQLRec 会把节点数据推送到 OTLP gRPC 接收端；日志写入标准输出和文件。

## 先看哪里

| 现象 | 先看 | 需要进一步定位时 |
| --- | --- | --- |
| API 报错或变慢 | Grafana 中的 HTTP 请求、节点耗时和错误面板 | 开启本次请求的 Trace，查看日志 |
| 推荐结果为空或数量异常 | UI 中的执行 DAG、节点平均输出行数 | 查看本次请求的 Trace 和日志 |
| 数据源查询变慢 | 表扫描、主键查询或向量搜索面板 | 查看对应节点的 Trace，再检查数据源 |
| REST 服务没有响应 | 容器或 Pod 日志 | 检查服务端口和 `/metrics` |

## 查看运行趋势

### 直接检查指标

REST 服务在默认的 **30001** 端口提供 `GET /metrics`，与业务 API 共用端口。使用 [Docker Demo](../getting-started/docker.md) 时，先调用一次 API，再读取指标：

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H 'Content-Type: application/json' \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'

curl http://localhost:30001/metrics
```

返回的是 Prometheus 文本格式。节点指标要在对应节点执行后才会出现。Docker Demo 的 CLI 与 REST 服务是两个进程，在 CLI 中执行 SQL 不会增加 REST 服务进程的节点指标。只启用 Thrift、关闭 REST 时，也没有 `/metrics` 这个 HTTP 入口。

### 在 Minikube 查看 Grafana

按[服务部署](./deployment.md)搭建的测试环境默认不部署监控。可在项目的 `deploy` 目录执行：

```bash
cd deploy
bash ./prometheus/init.sh
bash ./prometheus/deploy.sh
```

如果已执行过 `download_resource.sh`，`init.sh` 添加 Helm 仓库的步骤已经完成。部署脚本安装 Prometheus 和 Grafana，并创建 ServiceMonitor：它每 **15 秒**抓取同一命名空间中 `app=sqlrec` Service 的 `rest` 端口上的 `/metrics`。

Grafana 默认地址为 `http://<minikube ip>:30035`，Prometheus 为 `http://<minikube ip>:30036`；端口可通过 `GRAFANA_PORT`、`PROMETHEUS_PORT` 覆盖。Grafana 用户名为 `admin`，密码可通过以下命令读取；若修改了 `NAMESPACE`，请替换命令中的 `sqlrec`：

```bash
kubectl -n sqlrec get secret prometheus-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

### 看哪些面板

在 Grafana 的 **SQLRec** 仪表盘中，先选顶部的 `namespace`、`job`、`pod`，再按问题查看：

| 想看什么 | 面板 |
| --- | --- |
| API 请求耗时、请求速率和状态码 | HTTP Request Metrics |
| 节点耗时、成功/错误速率和输出行数 | Node Execution Duration、Node Execution Status、Node Data Size |
| 表查询与缓存分支 | Table Scan Metrics、Table Get By Primary Key Metrics、Table Vector Search Metrics、Cache Table Metrics、If Cache Metrics |
| SQL 定义更新、活动会话和操作 | Function Update Metrics、Session and Operation Metrics |

**SQLRec JVM** 仪表盘用于看内存、GC、CPU 和线程。它包含一些通用面板；例如 `Log Events` 使用的 `logback_events_total` 并非 SQLRec 当前提供的指标，面板空白时先在 `/metrics` 中确认指标是否存在。仓库脚本只提供采集和仪表盘，没有配置 SQLRec 专用告警规则。

SQLRec UI 的 `/ui/static/index.html` 也能查看函数执行 DAG，以及当前进程中节点的平均耗时和平均输出行数。Docker Demo 可访问 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)。这些值来自已记录的成功执行，是历史平均值，不能代表某一次请求。

### 看不到数据时

1. 访问 SQLRec 的 `/metrics`；调用一次 API 后，确认能看到节点指标。
2. 在 Prometheus 的 **Status → Targets** 中确认 SQLRec 目标为 `UP`。若没有目标，检查 ServiceMonitor 是否匹配 SQLRec Service 的命名空间、`app=sqlrec` 标签和 `rest` 端口名。
3. 在 Grafana 选对 `namespace`、`job`、`pod` 和时间范围。SQLRec 仪表盘的选项来自节点指标；尚未执行节点时可能为空，低流量下速率图也可能暂时没有数据。

## 定位一次请求

成功的 API 响应会在 `params.log_id` 中带上本次执行的 ID。开启 Trace 后，可用它查找 span 的 `log.id` 属性；启用调试日志时，节点日志也带有这个 ID。请求失败而没有返回 ID 时，可按请求时间查看服务日志。

### 查看 Trace

Trace 默认关闭。先确保 `TRACE_ENDPOINT` 指向可访问的 OTLP gRPC 接收端，再在需要排查的 API 请求体中加入 `DEBUG_TRACE`：

```json
{
  "data": {"user_info": [{"user_id": 1000001}]},
  "params": {"DEBUG_TRACE": "true"}
}
```

如需持续采集，可把服务环境变量 `DEBUG_TRACE` 设为 `true` 并重新部署。Minikube 测试环境可以单独部署 Jaeger：

```bash
cd deploy
bash ./jaeger/deploy.sh
```

`deploy/sqlrec/deploy.sh` 配置的默认 `TRACE_ENDPOINT` 指向 Jaeger 的 OTLP gRPC NodePort。Jaeger UI 默认地址为 `http://<minikube ip>:30037`；若覆盖了端点或端口，以实际配置为准。Trace 使用批量导出，数据可能在请求结束后稍晚出现。

每个节点 span 记录 `log.id`、`duration.ms`、`data.count`、`status`，失败时还会记录错误。当前实现不提取上游 HTTP Trace 上下文，也不向外部数据源或模型服务传播，因此看到的是 SQLRec 内部节点链。

### 查看日志

Docker Demo 使用 `docker logs -f sqlrec-demo`；Kubernetes 部署使用 `kubectl logs -n sqlrec deployment/sqlrec`，并按实际 `NAMESPACE` 修改命名空间。SQLRec 还写入容器内的 `/var/log/sqlrec/sqlrec.log`；`LOG_DIR` 和 `LOG_LEVEL` 可修改文件目录和日志级别。

需要节点调试信息时，可在单次 API 请求的 `params` 中加入 `"DEBUG_PRINT":"true"`，或设置服务环境变量 `DEBUG_PRINT=true`。调试日志可能包含 SQL 查询结果等业务数据，并增加日志量，应只在排查时启用。

## 接入现有监控平台

公有云可以接入已有的 Prometheus 兼容采集和 Trace 服务；私有云可以运行自己的 Prometheus、Grafana、OpenTelemetry Collector 和 Trace 后端。仓库中的 Minikube、NodePort 与 Jaeger 脚本只是测试环境示例。部署时需要分别打通**采集器到 SQLRec REST 端口**和 **SQLRec 到 OTLP gRPC 接收端**的网络连接。

### 配置指标采集

让采集器从内网访问 `http://<sqlrec-host>:30001/metrics`；若修改了 `SQLREC_REST_PORT`，使用实际端口。`/metrics` 没有独立鉴权，应限制访问，不要直接暴露到公网；参见 [Prometheus 安全模型](https://prometheus.io/docs/operating/security/)。

- **使用 Prometheus Operator**：参考 `deploy/prometheus/sqlrec-servicemonitor.yaml`，按实际命名空间、Service 标签和端口名调整。还要确认平台 Prometheus 的 `serviceMonitorSelector`、`serviceMonitorNamespaceSelector` 会选中它；示例中的 `release: prometheus` 是本仓库的 Helm 安装约定。[Operator 文档](https://prometheus-operator.dev/docs/getting-started/design/)解释了选择关系。
- **使用普通 Prometheus 或采集代理**：抓取相同端点。单实例配置示例：

```yaml
scrape_configs:
  - job_name: sqlrec
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ['sqlrec.example.internal:30001']
```

替换为采集器实际能访问的地址；多实例需要发现并抓取每个实例。可导入 `deploy/prometheus/sqlrec_grafana.json` 和 `jvm_grafana.json`，但非 Kubernetes 的静态抓取默认没有 `namespace`、`pod` 标签，需要补充标签或调整仪表盘筛选条件。更多抓取选项见 [Prometheus 配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)。

### 配置 Trace 导出

在 SQLRec 的 Deployment 或运行配置中设置下方[Trace 配置](#trace-配置)列出的变量。`TRACE_ENDPOINT` 必须是 SQLRec 可访问的 **OTLP gRPC** 地址；不要把 OTLP/HTTP 的 `/v1/traces` 路径加到地址后。公有云使用平台提供的兼容端点；私有云可指向内部 Collector 或 Jaeger 服务。自建 Collector 时，需要启用 OTLP gRPC 接收器和发往后端的 `traces` 管道；参见 [Collector 文档](https://opentelemetry.io/docs/collector/architecture/)。

若接收端需要鉴权，仓库的 `deploy/sqlrec/sqlrec.yaml` 还需增加 `TRACE_HEADERS` 环境变量；现有示例只传入 `TRACE_ENDPOINT`、`TRACE_SERVICE_NAME`、`DEBUG_TRACE`。在 Kubernetes 中可从 Secret 注入：

```yaml
- name: TRACE_HEADERS
  valueFrom:
    secretKeyRef:
      name: sqlrec-observability
      key: trace-headers
```

使用 `https` 时，SQLRec 容器中的 JVM 还需要信任接收端证书。配置后，发送一次带 `DEBUG_TRACE` 的请求，在 Trace 后端按服务名或 `log.id` 查找；若没有数据，检查端点协议、连通性、鉴权和 SQLRec 日志。[OpenTelemetry OTLP 文档](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/)说明了 gRPC 与 HTTP 端点的区别。

## 配置速查

### 常用指标

Prometheus 暴露的指标名使用下划线；计数器带 `_total`，计时器包含 `_seconds_sum` 和 `_seconds_count`。

| 用途 | 指标 |
| --- | --- |
| HTTP 请求量与状态码 | `sqlrec_http_request_count_total` |
| HTTP 平均耗时 | `sqlrec_http_request_duration_seconds_sum`、`sqlrec_http_request_duration_seconds_count` |
| 节点平均耗时与状态 | `sqlrec_node_exec_duration_seconds_sum`、`sqlrec_node_exec_duration_seconds_count` |
| 表扫描耗时 | `sqlrec_table_scan_duration_seconds_sum`、`sqlrec_table_scan_duration_seconds_count` |
| 缓存分支超时 | `sqlrec_if_cache_timeout_total` |

平均耗时用对应的 `_sum` 除以 `_count`，单位为秒。主键查询和向量搜索还分别有 `sqlrec_table_get_by_primary_key_duration_seconds_*`、`sqlrec_table_vector_search_duration_seconds_*` 指标。例如，比较各节点最近五分钟的平均耗时：

```text
sum by (name) (rate(sqlrec_node_exec_duration_seconds_sum[5m]))
/
sum by (name) (rate(sqlrec_node_exec_duration_seconds_count[5m]))
```

HTTP 指标的 `path` 会归一化，例如 `/api/v1/demo_rec` 记为 `/api/v1/{apiName}`，不能用于区分具体 API。请求体中的 `metricTags` 可给节点执行等指标附加标签，但 HTTP 指标和部分 Connector 指标不会继承它。只使用取值数量有限的标签，如固定场景；不要传用户 ID、请求 ID。请求格式见[发布和调用 API](../guides/api.md)。

### Trace 配置

| 环境变量 | 用法 |
| --- | --- |
| `TRACE_ENDPOINT` | OTLP gRPC 接收端，如 `https://trace.example.internal:4317`；使用实际地址和端口 |
| `TRACE_SERVICE_NAME` | 稳定的服务名，如 `sqlrec-prod`，供 Trace 后端筛选 |
| `DEBUG_TRACE` | 默认 `false`；也可通过单次 API 请求的 `params` 开启 |
| `TRACE_HEADERS` | 接收端需要鉴权时设置，格式为 `key1=value1,key2=value2`；建议从密钥服务注入 |

这些是 SQLRec 自身读取的配置，通用的 `OTEL_EXPORTER_OTLP_*` 环境变量不能直接替代。
