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

两份 Grafana 仪表盘默认聚合当前筛选范围内的所有实例。先选顶部的 `namespace`、`job` 确定范围；`pod`、`instance` 保持 All 时查看整体，选择单个实例后，同一组面板即展示该实例。业务面板保留 path、api、节点名、表名等维度。在 **SQLRec** 仪表盘中按问题查看：

| 想看什么 | 面板 |
| --- | --- |
| API 请求耗时、速率、状态、队列 | Service overview、API and SQL outcomes |
| 节点耗时、状态、输出行数和降级 | Node execution |
| 表查询、写入 QPS 与失败率、缓存命中与容量 | Tables |
| 函数刷新、会话/句柄、特征覆盖 | Control plane and feature quality |

**SQLRec JVM** 默认展示内存、当前线程数、文件描述符等总用量；CPU 展示实例平均值和最高值，堆展示整体使用率和单实例最高使用率，FD 使用率及 GC 暂停占比展示单实例最高值。平均暂停按总耗时/总次数计算，最长暂停取最大值。内存池、线程状态等语义维度保留；uptime、历史线程峰值、类数和系统负载放在默认折叠的 Instance details 中，选择实例后展开查看。虚拟线程 scheduler Gauge 需要 Java 24+，FD 指标取决于平台；GC 事件发生前部分指标可能不存在。仓库未配置业务 SLO 告警规则。

业务执行器展示线程总数、总积压及单执行器最大积压；缓存条目数按表汇总各实例副本，包含重复 key，不代表去重数据量；Session/Operation 展示持有数量总和。

表写入 QPS 折线图展示速率窗口内平均每秒完成的调用次数，按表、操作和 outcome 分组；一次批量写入计一次，不代表写入行数。失败率按速率窗口计算，为同一表、同一操作的非 success 调用数占全部调用数的比例，包含异常和超时；返回 false 但未抛异常仍记为 success。没有失败时显示 0%，没有写入流量时显示 N/A。

SQLRec UI 的 `/ui/static/index.html` 也能查看函数执行 DAG，以及当前进程中节点的平均耗时和平均输出行数。Docker Demo 可访问 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)。这些值来自已记录的成功执行，是历史平均值，不能代表某一次请求。

### 看不到数据时

1. 访问 SQLRec 的 `/metrics`；调用一次 API 后，确认能看到节点指标。
2. 在 Prometheus 的 **Status → Targets** 中确认 SQLRec 目标为 `UP`。若没有目标，检查 ServiceMonitor 是否匹配 SQLRec Service 的命名空间、`app=sqlrec` 标签和 `rest` 端口名。
3. 在 Grafana 选对 `namespace`、`job`、`pod` 和时间范围。目标筛选来自 `up{service="sqlrec"}`，空闲启动也能选择；低流量下速率图仍可能暂时没有数据。

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

让采集器从内网访问 `http://<sqlrec-host>:30001/metrics`；若修改了 `SQLREC_REST_PORT`，使用实际端口。`GET /metrics` 始终允许匿名访问，开启 `AUTH_ENABLED` 后也无需给采集器配置账号密码。应限制访问，不要直接暴露到公网；参见 [Prometheus 安全模型](https://prometheus.io/docs/operating/security/)。

- **使用 Prometheus Operator**：参考 `deploy/prometheus/sqlrec-servicemonitor.yaml`，按实际命名空间、Service 标签和端口名调整。还要确认平台 Prometheus 的 `serviceMonitorSelector`、`serviceMonitorNamespaceSelector` 会选中它；示例中的 `release: prometheus` 是本仓库的 Helm 安装约定。[Operator 文档](https://prometheus-operator.dev/docs/getting-started/design/)解释了选择关系。
- **使用普通 Prometheus 或采集代理**：抓取相同端点。单实例配置示例：

```yaml
scrape_configs:
  - job_name: sqlrec
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ['sqlrec.example.internal:30001']
        labels:
          service: sqlrec
```

替换为采集器实际能访问的地址；多实例需要发现并抓取每个实例。可导入 `deploy/prometheus/sqlrec_grafana.json` 和 `jvm_grafana.json`。面板限定 `service="sqlrec"`，静态抓取需如上补充该标签；非 Kubernetes 部署没有 namespace/pod 时，保留这些筛选为 All，使用 instance 选择实例。数据源的 scrape interval/Min step 设为 15s，或按实际抓取频率调整。更多抓取选项见 [Prometheus 配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)。

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
| HTTP 请求量与状态码 | `sqlrec_http_request_duration_seconds_count` |
| HTTP 平均耗时 | `sqlrec_http_request_duration_seconds_sum`、`sqlrec_http_request_duration_seconds_count` |
| 业务执行器线程数 / 待处理任务数 | `netty_eventexecutor_workers`、`netty_eventexecutor_tasks_pending`，筛选 `pool="rest-business"` |
| 节点平均耗时与状态 | `sqlrec_node_exec_duration_seconds_sum`、`sqlrec_node_exec_duration_seconds_count` |
| 表扫描耗时 | `sqlrec_table_scan_duration_seconds_sum`、`sqlrec_table_scan_duration_seconds_count` |
| TIMEIN 非正数预算直接降级 | `sqlrec_if_timein_outcome_total{reason="direct"}` |
| TIMEIN 等待 THEN 超时 | `sqlrec_if_timein_outcome_total{reason="timeout"}` |
| TIMEIN 普通异常回退 | `sqlrec_if_timein_outcome_total{reason="error"}` |

平均耗时用对应的 `_sum` 除以 `_count`，单位为秒。主键查询和向量搜索还分别有 `sqlrec_table_get_by_primary_key_duration_seconds_*`、`sqlrec_table_vector_search_duration_seconds_*` 指标。例如，比较各节点最近五分钟的平均耗时：

```text
sum by (name) (rate(sqlrec_node_exec_duration_seconds_sum[5m]))
/
sum by (name) (rate(sqlrec_node_exec_duration_seconds_count[5m]))
```

HTTP 指标中，`/api/v1/` 开头的请求保留完整路径，仅去掉查询参数；例如 `/api/v1/demo_rec?trace=true` 记为 `/api/v1/demo_rec`。其他路径使用 `/sql/v1`、`/metrics`、`/ui/api/*`、`/ui/static/*` 和未知路径 `unmatched` 分组。`method` 保留请求的原始方法名，不进行转换。具体 API 的执行结果使用 `sqlrec_api_execution_duration_seconds_*{api="demo_rec",outcome="success"}`；SQL statement 使用 `sqlrec_sql_execution_duration_seconds_*`，可以观察 HTTP 200 内部的 SQL 失败。节点与表执行统一使用 `outcome` 标签，节点取消另有 `phase=before_start/running`，不能把取消事件与 Timer 的 cancelled 次数相加。

在进程环境中配置 `METRICS_TAG_KEYS=scene,experiment`，统一打点 helper 会从当前执行变量提取同名值，直接导出为 `scene`、`experiment`。配置为空时不附加业务标签；解析时 trim、忽略空项并去重，配置在启动时固定，请求不能覆盖。缺失、null、空白值统一为空字符串；与当前指标的系统标签同名时，系统标签优先。未配置变量（包括 log_id）不会自动导出。HTTP 与表指标不继承业务维度，API/SQL/节点、分支/降级、特征覆盖指标支持它。打点时读取当前变量值，同一事件复用一份标签快照；不会更改业务原值。直方图与注册失败回调在实际 Prometheus registry 初始化时配置，composite 仅负责分发指标。

业务标签非空值按原值记录，不限制维度数、取值数量、长度、组合数或 meter 总量，也不设置变量名黑名单。配置键保留合法标签名的格式校验。HTTP method 使用请求中的原值；SQL statement_type 使用首个词的大写形式，空 SQL 记为空字符串，不做白名单归并。`sqlrec_metrics_registration_failures_total` 记录注册失败。

HTTP、API 和主要表查询默认提供有限直方图桶；节点按 `METRICS_HISTOGRAM_NODES=demo_rec:RETURN,demo_rec:items` 精确名称开启，默认关闭节点桶。桶边界为 5/10/25/50/100/250/500/1000/3000/10000/30000/60000ms，另有 +Inf；分位数精度受桶宽限制，超过最大有限桶的分位数也不能准确表示。

HTTP 仅保留 request duration Timer，从业务 Handler 开始执行计到响应写操作完成（含写失败），不包含此前业务队列等待或客户端网络全程。QPS、状态码比例、平均耗时、P95/P99 均来自这个 Timer；进入 Handler 前被拒绝的请求不计入。使用 Netty 内置业务线程分发，队列满时沿用 Netty 的拒绝机制，不保证返回 HTTP 503。线程和队列使用官方 `NettyEventExecutorMetrics`，固定 `pool="rest-business"`；workers 是工作线程总数，pending 按线程 `name` 导出，包含 HTTP 以外的 pipeline 任务。不额外采集排队/处理耗时、在途请求、拒绝或写失败计数。

表缓存启用 Caffeine `recordStats()`，通过 Micrometer 官方 `CaffeineCacheMetrics` 采集。面板使用 `cache_gets_total{result="hit|miss"}`、`cache_size`、`cache_evictions_total`，固定 `cache`/`table` 标签均为完整表名。命中率合并 read-through/cache-only 两种读取，未启用缓存的请求不计入；不增加容量或后端加载耗时等自定义缓存指标。表刷新创建新缓存时，先注销同表旧指标，再绑定新实例；新实例关闭缓存时只注销旧指标。成功刷新表清单并确认某表已删除时，注销该表的缓存指标；读取元数据失败不触发删表注销。未变化的表复用原缓存及其指标。新缓存的累计计数重新开始；Prometheus 已存储的历史数据保留，跨实例切换的窗口不能视为精确连续计数。条目数为估算值，非字节占用。

UI 的成功节点均值按所有业务标签组合的总量/次数加权计算，仍是本进程累计历史值。

### Trace 配置

| 环境变量 | 用法 |
| --- | --- |
| `TRACE_ENDPOINT` | OTLP gRPC 接收端，如 `https://trace.example.internal:4317`；使用实际地址和端口 |
| `TRACE_SERVICE_NAME` | 稳定的服务名，如 `sqlrec-prod`，供 Trace 后端筛选 |
| `DEBUG_TRACE` | 默认 `false`；也可通过单次 API 请求的 `params` 开启 |
| `TRACE_HEADERS` | 接收端需要鉴权时设置，格式为 `key1=value1,key2=value2`；建议从密钥服务注入 |

这些是 SQLRec 自身读取的配置，通用的 `OTEL_EXPORTER_OTLP_*` 环境变量不能直接替代。
