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

In Grafana's **SQLRec** dashboard, select `namespace`, `job`, and `pod` at the top, then use the relevant panels:

| What to inspect | Panels |
| --- | --- |
| API latency, request rate, and status codes | HTTP Request Metrics |
| Node duration, success/error rates, and output counts | Node Execution Duration, Node Execution Status, Node Data Size |
| Table queries and cache branches | Table Scan Metrics, Table Get By Primary Key Metrics, Table Vector Search Metrics, Cache Table Metrics, If Cache Metrics |
| SQL definition updates, active sessions, and operations | Function Update Metrics, Session and Operation Metrics |

The **SQLRec JVM** dashboard covers memory, GC, CPU, and threads. It also contains generic panels. For example, `Log Events` uses `logback_events_total`, which SQLRec does not currently expose; if a panel is empty, first check whether its metric exists in `/metrics`. The repository scripts set up scraping and dashboards but no SQLRec-specific alert rules.

The SQLRec UI at `/ui/static/index.html` shows a function's execution DAG and the average node duration and output count recorded in the current process. For the Docker Demo, open [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html). These are historical averages from successful executions, not values for one request.

### When Data Is Missing

1. Open SQLRec's `/metrics`. After an API call, check that node metrics appear.
2. In Prometheus, open **Status → Targets** and check that the SQLRec target is `UP`. If the target is absent, check whether the ServiceMonitor matches the SQLRec Service's namespace, `app=sqlrec` label, and `rest` port name.
3. In Grafana, select the right `namespace`, `job`, `pod`, and time range. The SQLRec dashboard options come from node metrics, so they may be empty before a node runs. Rate graphs may also be empty with very little traffic.

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
```

Replace the address with one the collector can reach. Discover and scrape every instance in a multi-instance deployment. You can import `deploy/prometheus/sqlrec_grafana.json` and `jvm_grafana.json`. Static scraping outside Kubernetes does not provide `namespace` or `pod` labels by default, so add those labels or adjust the dashboard filters. See the [Prometheus configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) for other discovery options.

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
| HTTP request count and status | `sqlrec_http_request_count_total` |
| Mean HTTP duration | `sqlrec_http_request_duration_seconds_sum`, `sqlrec_http_request_duration_seconds_count` |
| Mean node duration and status | `sqlrec_node_exec_duration_seconds_sum`, `sqlrec_node_exec_duration_seconds_count` |
| Table scan duration | `sqlrec_table_scan_duration_seconds_sum`, `sqlrec_table_scan_duration_seconds_count` |
| Cache branch timeouts | `sqlrec_if_cache_timeout_total` |

Divide a timer's `_sum` by its `_count` to get the mean duration in seconds. Primary-key lookups and vector searches also expose `sqlrec_table_get_by_primary_key_duration_seconds_*` and `sqlrec_table_vector_search_duration_seconds_*`. For example, compare each node's mean duration over the last five minutes:

```text
sum by (name) (rate(sqlrec_node_exec_duration_seconds_sum[5m]))
/
sum by (name) (rate(sqlrec_node_exec_duration_seconds_count[5m]))
```

HTTP metric `path` values are normalized: `/api/v1/demo_rec` becomes `/api/v1/{apiName}`, so they do not distinguish individual APIs. Request-body `metricTags` can label node execution and other context-aware metrics, but HTTP metrics and some Connector metrics do not inherit them. Use values with a small, fixed set of possibilities, such as a known scene; avoid user and request IDs. See [Publishing and Calling APIs](../guides/api.md) for the request format.

### Trace Settings

| Environment variable | Usage |
| --- | --- |
| `TRACE_ENDPOINT` | OTLP gRPC receiver, such as `https://trace.example.internal:4317`; use the actual address and port |
| `TRACE_SERVICE_NAME` | Stable service name, such as `sqlrec-prod`, for filtering in the trace backend |
| `DEBUG_TRACE` | Defaults to `false`; it can also be enabled through one API request's `params` |
| `TRACE_HEADERS` | Set when the receiver requires header authentication, in `key1=value1,key2=value2` format; inject it from a secret store |

SQLRec reads these settings directly. Generic `OTEL_EXPORTER_OTLP_*` environment variables do not replace them.
