# Observability

Use **metrics** to track request and node trends over time, **traces** to inspect one request's execution inside SQLRec, and **logs** to investigate errors and debug details. A collector pulls metrics from `/metrics`. When tracing is enabled, SQLRec pushes node spans to an OTLP gRPC receiver. Logs go to standard output and a file.

## Where to Start

| Symptom | Start here | Then |
| --- | --- | --- |
| An API fails or slows down | HTTP request, node duration, and error panels in Grafana | Trace that request and inspect logs |
| Recommendation results are empty or unexpected | The execution DAG and average node output counts in the UI | Inspect the request's trace and logs |
| Data source queries slow down | Table scan, primary-key lookup, or vector-search panels | Inspect the relevant node trace, then the data source |
| The REST service does not respond | Container or Pod logs | Check the service port and `/metrics` |

## Monitor Trends

### Check Metrics Directly

The REST server exposes `GET /metrics` on port **30001** by default, alongside business APIs. With the [Docker Demo](../getting-started/docker.md), call an API once before reading its metrics:

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H 'Content-Type: application/json' \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'

curl http://localhost:30001/metrics
```

The response uses Prometheus text format. Node metrics appear only after the corresponding nodes run. The Docker Demo CLI and REST server are separate processes, so SQL executed in the CLI does not update the REST process's node metrics. If only Thrift is enabled and REST is disabled, there is no HTTP `/metrics` endpoint.

### View Grafana in Minikube

The test environment in [Service Deployment](./deployment.md) does not install monitoring by default. From the repository root, run:

```bash
cd deploy
bash ./prometheus/init.sh
bash ./prometheus/deploy.sh
```

If you already ran `download_resource.sh`, its Helm repository setup includes the `init.sh` step. The deployment installs Prometheus and Grafana and creates a ServiceMonitor. It scrapes `/metrics` every **15 seconds** from the `rest` port of a Service labeled `app=sqlrec` in the same namespace.

Grafana is available at `http://<minikube ip>:30035` and Prometheus at `http://<minikube ip>:30036` by default. Override the ports with `GRAFANA_PORT` and `PROMETHEUS_PORT`. The Grafana username is `admin`. To read its password, run the following command, replacing `sqlrec` if you changed `NAMESPACE`:

```bash
kubectl -n sqlrec get secret prometheus-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

### Which Dashboards to Use

Both Grafana dashboards aggregate all instances in the selected scope by default. Select `namespace` and `job` first; leave `pod` and `instance` at All for an overview, or select one instance to drill down using the same panels. Business panels retain path, API, node and table dimensions. In the **SQLRec** dashboard, use the relevant panels:

| What to inspect | Panels |
| --- | --- |
| API latency, request rate, and status codes | Service overview, API and SQL outcomes |
| Node duration, success/error rates, and output counts | Node execution |
| Table queries, write QPS and failure ratios, and caches | Tables |
| SQL definition updates, active sessions, and operations | Control plane and feature quality |

The **SQLRec JVM** dashboard defaults to total memory, current threads and open file descriptors. CPU shows instance averages and maxima; heap utilization shows overall capacity-weighted usage and the highest instance usage. FD utilization and GC pause fraction show the highest instance value. Mean GC pause uses total duration / total collection count; maximum pauses use the maximum. Memory-pool and thread-state dimensions remain. Uptime, historical thread peaks, loaded classes and system load are in the collapsed Instance details section; select an instance before expanding it. Scheduler gauges require Java 24+, FD metrics are platform-dependent, and some GC metrics appear only after an event. Business SLO alert rules are not configured.

Business executors show total workers, total pending tasks and the largest individual executor queue. Cache entries sum replicas per table, including duplicate keys, rather than counting unique data. Session/Operation panels show total held handles.

The table write QPS line chart shows average completed calls per second over the rate interval, grouped by table, operation and outcome. Each batch counts as one call, not its row count. The failure ratio uses the rate interval and divides non-successful calls by all calls for the same table and operation, including errors and timeouts. Returning false without throwing is still recorded as success. No failures gives 0%; no write traffic gives N/A.

The SQLRec UI at `/ui/static/index.html` shows a function's execution DAG and the average node duration and output count recorded in the current process. For the Docker Demo, open [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html). These are historical averages from successful executions, not values for one request.

### When Data Is Missing

1. Open SQLRec's `/metrics`. After an API call, check that node metrics appear.
2. In Prometheus, open **Status → Targets** and check that the SQLRec target is `UP`. If the target is absent, check whether the ServiceMonitor matches the SQLRec Service's namespace, `app=sqlrec` label, and `rest` port name.
3. In Grafana, select the right `namespace`, `job`, `pod`, and time range. Target filters use `up{service="sqlrec"}` and work before nodes run. Rate graphs may still be empty with very little traffic.

## Investigate One Request

A successful API response includes the execution ID in `params.log_id`. With tracing enabled, use it to find a span's `log.id` attribute. Node debug logs also include that ID. If a failed request returns no ID, inspect the service logs around the request time.

### View a Trace

Tracing is disabled by default. First point `TRACE_ENDPOINT` to a reachable OTLP gRPC receiver. Then add `DEBUG_TRACE` to the API request you want to investigate:

```json
{
  "data": {"user_info": [{"user_id": 1000001}]},
  "params": {"DEBUG_TRACE": "true"}
}
```

To trace requests continuously, set the `DEBUG_TRACE` service environment variable to `true` and redeploy. In the Minikube test environment, you can install Jaeger separately:

```bash
cd deploy
bash ./jaeger/deploy.sh
```

The default `TRACE_ENDPOINT` set by `deploy/sqlrec/deploy.sh` points to Jaeger's OTLP gRPC NodePort. The Jaeger UI is at `http://<minikube ip>:30037` by default. Use the actual endpoint and port if you changed them. Spans are exported in batches, so they may appear shortly after the request finishes.

Each node span records `log.id`, `duration.ms`, `data.count`, and `status`, plus error details when a node fails. The current implementation does not extract upstream HTTP trace context or propagate context to external data sources or model services. The trace therefore covers SQLRec's internal node chain.

### Read Logs

For the Docker Demo, use `docker logs -f sqlrec-demo`. In Kubernetes, use `kubectl logs -n sqlrec deployment/sqlrec`, replacing the namespace if `NAMESPACE` differs. SQLRec also writes `/var/log/sqlrec/sqlrec.log` inside the container. Use `LOG_DIR` and `LOG_LEVEL` to change the file directory and logging level.

For node debug output, add `"DEBUG_PRINT":"true"` to `params` in one API request or set the service environment variable `DEBUG_PRINT=true`. Debug logs can include SQL query results and increase log volume, so enable them only while investigating a problem.

## Connect an Existing Monitoring Platform

In a public cloud, SQLRec can use an existing Prometheus-compatible collector and trace service. In a private cloud, you can run Prometheus, Grafana, an OpenTelemetry Collector, and a trace backend yourself. The repository's Minikube, NodePort, and Jaeger scripts are test-environment examples. In either setting, allow network traffic **from the collector to SQLRec's REST port** and **from SQLRec to the OTLP gRPC receiver**.

### Configure Metric Scraping

Let the collector reach `http://<sqlrec-host>:30001/metrics` over a private network. Use the actual port if you changed `SQLREC_REST_PORT`. The endpoint has no separate authentication, so restrict access and avoid exposing it directly to the public internet; see the [Prometheus security model](https://prometheus.io/docs/operating/security/).

- **With Prometheus Operator:** Use `deploy/prometheus/sqlrec-servicemonitor.yaml` as a starting point. Adapt its namespace, Service labels, and port name. Ensure the platform's Prometheus `serviceMonitorSelector` and `serviceMonitorNamespaceSelector` select it. The example's `release: prometheus` label belongs to this repository's Helm setup. The [Operator documentation](https://prometheus-operator.dev/docs/getting-started/design/) explains these selectors.
- **With plain Prometheus or a scraping agent:** Scrape the same endpoint. For one instance, a configuration looks like this:

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

Replace the address with one the collector can reach. Discover and scrape every instance in a multi-instance deployment. You can import `deploy/prometheus/sqlrec_grafana.json` and `jvm_grafana.json`. Dashboards select `service="sqlrec"`, so include that label as shown above. Outside Kubernetes, leave missing namespace/pod filters at All and select an instance. Set the datasource scrape interval/Min step to 15s, or to the actual scraping interval. See the [Prometheus configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) for other discovery options.

### Configure Trace Export

Set the variables in [Trace Settings](#trace-settings) in the SQLRec Deployment or other runtime configuration. `TRACE_ENDPOINT` must be an **OTLP gRPC** address reachable from SQLRec; do not append the OTLP/HTTP `/v1/traces` path. In a public cloud, use the platform's compatible endpoint. In a private cloud, point it at an internal Collector or Jaeger service. A self-hosted Collector needs an OTLP gRPC receiver and a `traces` pipeline that exports to the trace backend; see the [Collector documentation](https://opentelemetry.io/docs/collector/architecture/).

If the receiver requires authentication, add `TRACE_HEADERS` to the container environment in `deploy/sqlrec/sqlrec.yaml`. The example already passes `TRACE_ENDPOINT`, `TRACE_SERVICE_NAME`, and `DEBUG_TRACE`. In Kubernetes, credentials can come from a Secret:

```yaml
- name: TRACE_HEADERS
  valueFrom:
    secretKeyRef:
      name: sqlrec-observability
      key: trace-headers
```

For an `https` endpoint, the JVM in the SQLRec container must trust the receiver's certificate. After configuration, send a request with `DEBUG_TRACE` and search the trace backend by service name or `log.id`. If no span appears, check the endpoint protocol, network access, authentication, and SQLRec logs. The [OpenTelemetry OTLP documentation](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/) explains the difference between gRPC and HTTP endpoints.

## Configuration Reference

### Common Metrics

Prometheus metric names use underscores. Counters have the `_total` suffix; timers include `_seconds_sum` and `_seconds_count`.

| Purpose | Metric |
| --- | --- |
| HTTP request count and status | `sqlrec_http_request_duration_seconds_count` |
| Mean HTTP duration | `sqlrec_http_request_duration_seconds_sum`, `sqlrec_http_request_duration_seconds_count` |
| Business executor workers / pending tasks | `netty_eventexecutor_workers`, `netty_eventexecutor_tasks_pending`, filtered by `pool="rest-business"` |
| Mean node duration and status | `sqlrec_node_exec_duration_seconds_sum`, `sqlrec_node_exec_duration_seconds_count` |
| Table scan duration | `sqlrec_table_scan_duration_seconds_sum`, `sqlrec_table_scan_duration_seconds_count` |
| Direct TIMEIN fallback for a non-positive budget | `sqlrec_if_timein_outcome_total{reason="direct"}` |
| TIMEIN timeout while waiting for THEN | `sqlrec_if_timein_outcome_total{reason="timeout"}` |
| TIMEIN fallback after an ordinary THEN failure | `sqlrec_if_timein_outcome_total{reason="error"}` |

Divide a timer's `_sum` by its `_count` to get the mean duration in seconds. Primary-key lookups and vector searches also expose `sqlrec_table_get_by_primary_key_duration_seconds_*` and `sqlrec_table_vector_search_duration_seconds_*`. For example, compare each node's mean duration over the last five minutes:

```text
sum by (name) (rate(sqlrec_node_exec_duration_seconds_sum[5m]))
/
sum by (name) (rate(sqlrec_node_exec_duration_seconds_count[5m]))
```

HTTP `path` uses the actual request path without query parameters: `/api/v1/demo_rec?trace=true` is recorded as `/api/v1/demo_rec`. API, UI, unknown paths and requests whose query parameters fail to parse all retain their actual paths, without templates or shared placeholders. Per-API results use `sqlrec_api_execution_duration_seconds_*{api="demo_rec",outcome="success"}`. REST SQL statements use `sqlrec_sql_execution_duration_seconds_*`, including failures returned inside HTTP 200. Execution metrics use `outcome`; node cancellations have `phase=before_start/running`. Do not add cancellation events to the timer's cancelled count.

Set process environment `METRICS_TAG_KEYS=scene,experiment`. The metrics helper reads these current execution variables and exports `scene` and `experiment` directly. Empty configuration adds no business tags. Keys are trimmed, deduplicated and fixed at startup; requests cannot override the list. Missing, null and blank values become empty strings. Fixed system tags take precedence when keys conflict on the current metric. Unconfigured variables, including log_id, are never automatically exported. API/SQL/node, branch/fallback and feature metrics carry business tags; HTTP and table metrics do not. Each event reuses one tag snapshot, and normalization never changes the business variables. Histograms and registration-failure callbacks are configured on the actual Prometheus registry during initialization; the composite forwards meters to its child registries.

Nonempty business tag values are recorded unchanged. There are no limits on dimension count, value count, value length, combinations or meter count, and no variable-name denylist. Configured keys still require valid label-name syntax. HTTP method uses the request value; SQL statement_type uses the first word in uppercase, or an empty string for empty SQL, with no allowlist mapping. `sqlrec_metrics_registration_failures_total` counts registration failures.

HTTP, API and main table queries publish fixed histogram buckets. Enable node buckets with exact names in `METRICS_HISTOGRAM_NODES=demo_rec:RETURN,demo_rec:items`; disabled by default. Boundaries are 5/10/25/50/100/250/500/1000/3000/10000/30000/60000ms plus +Inf. Quantiles have bucket-width error and cannot accurately represent values beyond the highest finite bucket.

HTTP keeps a single request-duration Timer, from business-handler execution start to response-write completion, including failed writes. It excludes prior business-queue wait and the client's full network round trip. QPS, status ratios, mean latency and P95/P99 use this Timer; requests rejected before reaching the handler are excluded. Netty dispatches requests to the business executors; its rejection mechanism applies when the queue is full, with no guaranteed HTTP 503 response. The official `NettyEventExecutorMetrics` binder exports worker and pending-task counts tagged with `pool="rest-business"`. Workers means total workers; pending tasks are tagged by thread `name` and include other pipeline tasks. No separate queue/processing durations, in-flight gauge, rejection counter or write-failure counter are collected.

Table caches enable Caffeine `recordStats()` and bind through Micrometer's official `CaffeineCacheMetrics`. Panels use `cache_gets_total{result="hit|miss"}`, `cache_size` and `cache_evictions_total`; both `cache` and `table` tags contain the full table name. Hit ratio combines read-through and cache-only reads; cache-disabled calls are excluded. No custom capacity or backend-load metrics are added. When a table refresh creates a new cache, its old meters are removed before binding the new instance. A replacement with caching disabled only removes the old meters. A successful table-list refresh removes cache meters for tables confirmed deleted; failed metadata reads do not trigger deletion cleanup. Unchanged tables retain their cache and meters. Cumulative counts restart with the new cache; previously stored Prometheus history remains, and windows spanning replacement cannot provide exact continuous counts. Size is an estimate of entries, not bytes.

UI node means weight all business-tag combinations by total duration/rows and event count; they remain process-local cumulative means.

### Trace Settings

| Environment variable | Usage |
| --- | --- |
| `TRACE_ENDPOINT` | OTLP gRPC receiver, such as `https://trace.example.internal:4317`; use the actual address and port |
| `TRACE_SERVICE_NAME` | Stable service name, such as `sqlrec-prod`, for filtering in the trace backend |
| `DEBUG_TRACE` | Defaults to `false`; it can also be enabled through one API request's `params` |
| `TRACE_HEADERS` | Set when the receiver requires header authentication, in `key1=value1,key2=value2` format; inject it from a secret store |

SQLRec reads these settings directly. Generic `OTEL_EXPORTER_OTLP_*` environment variables do not replace them.
