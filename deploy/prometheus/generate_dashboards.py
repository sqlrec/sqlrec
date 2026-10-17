#!/usr/bin/env python3
"""Generate the checked-in dashboards: python3 deploy/prometheus/generate_dashboards.py.

Selectors use service=sqlrec, supplied by the ServiceMonitor (or static scrape labels).
Business tag filtering applies only to metrics that carry execution variables.
Overview panels aggregate selected instances; pod/instance filters provide drill-down.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "${DS_PROMETHEUS}"}
BASE = 'service="sqlrec",namespace=~"$namespace",job=~"$job",pod=~"$pod",instance=~"$instance"'
BUSINESS = ',${business_key:raw}=~"${business_value:regex}"'
WINDOW = "$__rate_interval"


def metric(name, labels="", business=False):
    return name + "{" + BASE + ("," + labels if labels else "") + (BUSINESS if business else "") + "}"


def rate(name, labels="", business=False):
    return f"rate({metric(name, labels, business)}[{WINDOW}])"


def aggregate(expr, groups="", op="sum"):
    return f"{op} by ({groups}) ({expr})" if groups else f"{op}({expr})"


def avg(name, groups, labels="", business=False):
    return (aggregate(rate(name + "_sum", labels, business), groups) + " / "
            + aggregate(rate(name + "_count", labels, business), groups))


def quantile(name, groups, quantile=0.99, labels="", business=False):
    return (f"histogram_quantile({quantile}, "
            + aggregate(rate(name + "_bucket", labels, business), "le," + groups) + ")")


def ratio(numerator, denominator):
    # Fill only a missing numerator for an existing denominator; no traffic remains N/A.
    return f"(({numerator}) or (0 * ({denominator}))) / ({denominator})"


def variable(name, query, label=None):
    return {"name": name, "label": label or name.title(), "type": "query", "datasource": DS,
            "query": query, "refresh": 1, "sort": 1, "multi": True, "includeAll": True,
            "allValue": ".*", "current": {"text": "All", "value": "$__all"}, "options": []}


class Dashboard:
    def __init__(self, title, uid, business=False):
        self.y = 0
        self.column = 0
        self.panels = []
        variables = [{"name": "DS_PROMETHEUS", "label": "Prometheus", "type": "datasource",
                      "query": "prometheus", "refresh": 1, "current": {}, "options": []}]
        variables += [
            variable("namespace", 'label_values(up{service="sqlrec"}, namespace)'),
            variable("job", 'label_values(up{service="sqlrec",namespace=~"$namespace"}, job)'),
            variable("pod", 'label_values(up{service="sqlrec",namespace=~"$namespace",job=~"$job"}, pod)'),
            variable("instance", 'label_values(up{service="sqlrec",namespace=~"$namespace",job=~"$job",pod=~"$pod"}, instance)'),
        ]
        if business:
            variables += [
                variable("api", f'label_values({metric("sqlrec_api_execution_duration_seconds_count")}, api)'),
                variable("node", f'label_values({metric("sqlrec_node_exec_duration_seconds_count")}, name)'),
                variable("table", 'label_values(' + metric("", '__name__=~"sqlrec_table_.*|cache_.*"') + ', table)'),
                {"name": "business_key", "label": "Business label", "type": "textbox",
                 "query": "scene", "current": {"text": "scene", "value": "scene"}},
                variable("business_value", f'label_values({metric("sqlrec_node_exec_duration_seconds_count")}, $business_key)', "Business value"),
            ]
        self.data = {"title": title, "uid": uid, "schemaVersion": 39, "version": 1,
                     "editable": True, "tags": ["sqlrec", "prometheus"], "timezone": "browser",
                     "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
                     "templating": {"list": variables}, "panels": self.panels,
                     "links": [{"title": "SQLRec", "url": "/d/sqlrec", "keepTime": True, "includeVars": True},
                               {"title": "SQLRec JVM", "url": "/d/sqlrec-jvm", "keepTime": True, "includeVars": True}]}

    def row(self, title, collapsed=False):
        if self.column:
            self.y += 8
            self.column = 0
        self.panels.append({"id": len(self.panels) + 1, "type": "row", "title": title,
                            "collapsed": collapsed, "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.y += 1

    def panel(self, title, queries, unit="short", description="", stat=False):
        if isinstance(queries, str):
            queries = [(queries, title)]
        panel = {"id": len(self.panels) + 1, "type": "stat" if stat else "timeseries", "title": title,
                 "description": description, "datasource": DS,
                 "gridPos": {"x": 12 * self.column, "y": self.y, "w": 12, "h": 8},
                 "fieldConfig": {"defaults": {"unit": unit, "noValue": "N/A",
                                              "custom": {"drawStyle": "line", "axisSoftMin": 0}}, "overrides": []},
                 "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                             "tooltip": {"mode": "multi"}},
                 "targets": [{"refId": chr(65 + i), "expr": expr, "legendFormat": legend,
                              "range": not stat, "instant": stat, "interval": "15s", "datasource": DS}
                             for i, (expr, legend) in enumerate(queries)]}
        if stat:
            panel["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "none"}
        self.panels.append(panel)
        self.column = (self.column + 1) % 2
        if not self.column:
            self.y += 8

    def save(self, filename):
        # Grafana stores collapsed-row children inside the row instead of at the top level.
        panels = []
        target = panels
        for panel in self.panels:
            if panel["type"] == "row":
                panels.append(panel)
                panel["panels"] = []
                target = panel["panels"] if panel["collapsed"] else panels
            else:
                target.append(panel)
        self.data["panels"] = panels
        Path(__file__).with_name(filename).write_text(json.dumps(self.data, indent=2, ensure_ascii=False) + "\n")


def business_dashboard():
    d = Dashboard("SQLRec", "sqlrec", business=True)
    d.row("Service overview — HTTP excludes metrics/UI; business filters apply to API/SQL/nodes/features")
    healthy = aggregate(metric("up"))
    targets = aggregate(metric("up"), op="count")
    d.panel("Scrape target health", [(healthy, "healthy"), (targets + " - " + healthy, "failed"), (targets, "total")], stat=True,
            description="Counts within the selected namespace/job/pod/instance scope. Missing targets require service-discovery/replica monitoring.")
    http = 'path=~"/api/v1/.*|/sql/v1"'
    d.panel("Business HTTP requests/s", [(aggregate(rate("sqlrec_http_request_duration_seconds_count", http), "path"), "{{path}}")], "ops",
            "Actual request paths without query strings, reaching response write completion, including failed writes. Requests rejected before the business handler are excluded.")
    d.panel("HTTP error ratio by status", [(aggregate(rate("sqlrec_http_request_duration_seconds_count", http + ',status=~"[45].."'), "status")
            + " / on() group_left " + aggregate(rate("sqlrec_http_request_duration_seconds_count", http)), "{{status}}")], "percentunit",
            "Each 4xx/5xx status is shown separately as a fraction of all business HTTP requests across selected instances. "
            "Statuses never observed have no series; no traffic yields N/A. SQL statement failures inside HTTP 200 are shown separately.")
    for p, title in [(0.95, "P95"), (0.99, "P99")]:
        d.panel(f"HTTP {title} server latency", [(quantile("sqlrec_http_request_duration_seconds", "path", p, http), "{{path}}")], "s",
                "From business handler execution start to response write completion. Excludes prior business queue wait and client network time. Fixed SLO buckets.")
    d.panel("HTTP average server latency", [(avg("sqlrec_http_request_duration_seconds", "path", http), "{{path}}")], "s",
            "Same timer as HTTP P95/P99; excludes queue wait before the business handler starts.")
    executor = 'pool="rest-business"'
    d.panel("Business executor workers", [(aggregate(metric("netty_eventexecutor_workers", executor)), "total")],
            description="Total official NettyEventExecutorMetrics workers across selected instances, not busy workers. Select pod/instance to drill down.")
    d.panel("Business executor pending tasks", [(aggregate(metric("netty_eventexecutor_tasks_pending", executor)), "total"),
            (aggregate(metric("netty_eventexecutor_tasks_pending", executor), op="max"), "maximum per executor")],
            description="Total pending tasks and the largest individual executor queue within the selected scope. Includes pipeline tasks, not only HTTP requests.")

    d.row("API and SQL outcomes")
    api = 'api=~"$api"'
    d.panel("API executions/s by outcome", [(aggregate(rate("sqlrec_api_execution_duration_seconds_count", api, True), "api,outcome"), "{{api}} {{outcome}}")], "ops")
    d.panel("API success ratio", [(ratio(aggregate(rate("sqlrec_api_execution_duration_seconds_count", api + ',outcome="success"', True), "api"),
            aggregate(rate("sqlrec_api_execution_duration_seconds_count", api, True), "api")), "{{api}}")], "percentunit")
    d.panel("API P99 latency", [(quantile("sqlrec_api_execution_duration_seconds", "api", labels=api, business=True), "{{api}}")], "s")
    d.panel("API average latency", [(avg("sqlrec_api_execution_duration_seconds", "api,outcome", api, True), "{{api}} {{outcome}}")], "s")
    d.panel("SQL statement outcomes/s", [(aggregate(rate("sqlrec_sql_execution_duration_seconds_count", business=True), "statement_type,outcome"), "{{statement_type}} {{outcome}}")], "ops",
            "REST /sql/v1 statements, including failures returned inside HTTP 200; not remote Flink job completion.")

    d.row("Node execution — timers count started executions; cancellation events are separate")
    node = 'name=~"$node"'
    d.panel("Node executions/s by outcome", [(aggregate(rate("sqlrec_node_exec_duration_seconds_count", node, True), "name,outcome"), "{{name}} {{outcome}}")], "ops")
    d.panel("Started node execution success ratio", [(ratio(aggregate(rate("sqlrec_node_exec_duration_seconds_count", node + ',outcome="success"', True), "name"),
            aggregate(rate("sqlrec_node_exec_duration_seconds_count", node, True), "name")), "{{name}}")], "percentunit")
    d.panel("Node average latency", [(avg("sqlrec_node_exec_duration_seconds", "name,outcome", node, True), "{{name}} {{outcome}}")], "s")
    d.panel("Node sliding-window maximum latency", [(aggregate(metric("sqlrec_node_exec_duration_seconds_max", node, True), "name", "max"), "{{name}}")], "s",
            "Micrometer time-window maximum; expires to zero. Not summed across pods and not an all-time maximum.")
    d.panel("Node P99 latency (opt-in)", [(quantile("sqlrec_node_exec_duration_seconds", "name", labels=node, business=True), "{{name}}")], "s",
            "Only nodes in METRICS_HISTOGRAM_NODES publish buckets. N/A is expected for other nodes.")
    d.panel("Successful node output rows per execution", [(avg("sqlrec_node_rows", "name", node + ',outcome="success"', True), "{{name}}")])
    d.panel("Node sliding-window maximum output rows", [(aggregate(metric("sqlrec_node_rows_max", node, True), "name", "max"), "{{name}}")])
    d.panel("Node cancellation events/s", [(aggregate(rate("sqlrec_node_cancelled_total", node, True), "name,phase"), "{{name}} {{phase}}")], "ops",
            "before_start does not enter the node timer; running also appears in timer outcome=cancelled. Do not add both metrics.")
    d.panel("Ignored cache-node exceptions/s", [(aggregate(rate("sqlrec_cache_table_ignore_exception_total", node, True), "name"), "{{name}}")], "ops")
    d.panel("IF branch decisions/s", [(aggregate(rate("sqlrec_if_cache_branch_total", node, True), "name,branch"), "{{name}} {{branch}}")], "ops",
            "Conditional IF only; TIMEIN decisions are shown below.")
    d.panel("TIMEIN path decisions/s", [(aggregate(rate("sqlrec_if_timein_outcome_total", node, True), "name,reason"), "{{name}} {{reason}}")], "ops",
            "primary_success/direct/timeout/error describe path selection. A fallback can still fail; direct is a configured policy.")
    d.panel("TIMEIN timeout/error fallback ratio", [(ratio(aggregate(rate("sqlrec_if_timein_outcome_total", node + ',reason=~"timeout|error"', True), "name"),
            aggregate(rate("sqlrec_if_timein_outcome_total", node, True), "name")), "{{name}}")], "percentunit",
            "Direct policy fallbacks are in the denominator but are not failures.")

    d.row("Tables — no request business labels; scan may include a PK lookup")
    table = 'table=~"$table"'
    for stem, title in [("scan", "Scan"), ("get_by_primary_key", "Primary-key query"), ("vector_search", "Vector search")]:
        name = "sqlrec_table_" + stem + "_duration_seconds"
        d.panel(f"{title} operations/s", [(aggregate(rate(name + "_count", table), "table,outcome"), "{{table}} {{outcome}}")], "ops")
        d.panel(f"{title} average / P99 latency", [(avg(name, "table", table), "{{table}} average"),
                (quantile(name, "table", labels=table), "{{table}} P99")], "s")
    for stem, title in [("scan_rows", "Scan output rows"), ("vector_search_rows", "Vector search output rows"),
                        ("get_by_primary_key_keys", "Requested distinct converted primary keys")]:
        d.panel(title + " per successful operation", [(avg("sqlrec_table_" + stem, "table", table + ',outcome="success"'), "{{table}}")])
    d.panel("Cache hit ratio", [(aggregate(rate("cache_gets_total", table + ',result="hit"'), "table")
            + " / " + aggregate(rate("cache_gets_total", table), "table"), "{{table}}")], "percentunit",
            "Caffeine statistics across read-through and cache-only lookups. Only the current cache instance is bound; counters restart when it is replaced.")
    d.panel("Cache lookups/s", [(aggregate(rate("cache_gets_total", table), "table,result"), "{{table}} {{result}}")], "ops")
    d.panel("Cache estimated entries", [(aggregate(metric("cache_size", table), "table"), "{{table}}")],
            description="Estimated entries across selected cache replicas, not unique keys or bytes. Expiry cleanup is asynchronous. Select pod/instance to inspect one cache.")
    d.panel("Cache evictions/s", [(aggregate(rate("cache_evictions_total", table), "table"), "{{table}}")], "ops",
            "Caffeine evictions exclude explicit invalidations.")
    write_count = "sqlrec_table_write_duration_seconds_count"
    d.panel("Table write QPS", [(aggregate(rate(write_count, table), "table,operation,outcome"),
            "{{table}} {{operation}} {{outcome}}")], "ops",
            "Completed write calls per second over the rate interval, grouped by table, operation and outcome. "
            "Each batch counts as one call, not its row count. Single and batch calls are combined.")
    d.panel("Table write failure ratio", [(ratio(aggregate(rate(write_count, table + ',outcome!="success"'), "table,operation"),
            aggregate(rate(write_count, table), "table,operation")), "{{table}} {{operation}}")], "percentunit",
            "Non-successful completed write calls / all completed write calls over the rate interval, grouped by table and operation. "
            "Failures include errors and timeouts; a call returning false without an exception is recorded as success. No traffic yields N/A.")
    d.panel("Table write average latency", [(avg("sqlrec_table_write_duration_seconds", "table,operation,batch,outcome", table), "{{table}} {{operation}} batch={{batch}} {{outcome}}")], "s")

    d.row("Control plane and feature quality")
    d.panel("Function refresh outcomes/s", [(aggregate(rate("sqlrec_function_update_duration_seconds_count"), "outcome"), "{{outcome}}")], "ops")
    d.panel("Function refresh average latency", [(avg("sqlrec_function_update_duration_seconds", "outcome"), "{{outcome}}")], "s")
    for family, title in [("session", "Session"), ("operation", "Operation handle")]:
        d.panel(title + "s held", [(aggregate(metric(f"sqlrec_{family}_active_count")), "total")], description="Total held handles across selected instances, not currently executing SQL statements. Requires Thrift.")
        d.panel(title + " open/close events/s", [(aggregate(rate(f"sqlrec_{family}_open_count_total")), "open"),
                (aggregate(rate(f"sqlrec_{family}_close_count_total")), "close")], "ops")
    d.panel("Feature coverage (sample-weighted)", [(aggregate(rate("sqlrec_feature_non_missing_total", business=True), "group,table,field") + " / "
            + aggregate(rate("sqlrec_feature_observed_total", business=True), "group,table,field"), "{{group}} {{table}} {{field}}")], "percentunit")
    d.panel("Feature empty batches/s", [(aggregate(rate("sqlrec_feature_empty_batches_total", business=True), "group,table"), "{{group}} {{table}}")], "ops")
    d.panel("Metric registration failures (last 5m)", aggregate(f'increase({metric("sqlrec_metrics_registration_failures_total")}[5m])'), stat=True)
    d.save("sqlrec_grafana.json")


def jvm_dashboard():
    d = Dashboard("SQLRec JVM", "sqlrec-jvm")
    d.row("JVM memory — totals across selected instances; select pod/instance to drill down")
    for area in ("heap", "nonheap"):
        d.panel(f"JVM {area} memory", [(aggregate(metric(f"jvm_memory_{kind}_bytes", f'area="{area}"')), kind)
                for kind in ("used", "committed")], "bytes", "Total across memory pools and selected instances; undefined pool maxima are not added.")
    heap_used = metric("sqlrec_jvm_heap_used_bytes")
    heap_max = metric("sqlrec_jvm_heap_max_bytes") + " > 0"
    heap_utilization = heap_used + " / (" + heap_max + ")"
    d.panel("Heap utilization", [(aggregate(heap_used + " and (" + heap_max + ")") + " / " + aggregate(heap_max), "overall"),
            (aggregate(heap_utilization, op="max"), "maximum per instance")], "percentunit",
            "Overall is total used / total maximum for instances with a defined maximum. Maximum shows the most utilized instance; values come from MemoryMXBean.")
    d.panel("Heap maximum", [(aggregate(heap_max), "total")], "bytes")
    d.panel("Memory pools", [(aggregate(metric("jvm_memory_used_bytes"), "area,id"), "{{area}} {{id}} used"),
            (aggregate(metric("jvm_memory_committed_bytes"), "area,id"), "{{area}} {{id}} committed")], "bytes")
    d.panel("Buffer pool memory", [(aggregate(metric("jvm_buffer_memory_used_bytes"), "id"), "{{id}} used"),
            (aggregate(metric("jvm_buffer_total_capacity_bytes"), "id"), "{{id}} capacity")], "bytes")
    d.panel("Buffer counts", [(aggregate(metric("jvm_buffer_count_buffers"), "id"), "{{id}}")])
    d.row("CPU, threads and descriptors")
    d.panel("CPU utilization", [(aggregate(metric(name) + " >= 0", op=op), family + " " + label)
            for name, family in [("process_cpu_usage", "process"), ("system_cpu_usage", "system")]
            for op, label in [("avg", "instance average"), ("max", "instance maximum")]], "percentunit",
            "Unweighted average and maximum of reported MXBean utilization; not a cluster CPU-capacity ratio. System values may be shared by colocated instances. Container throttling requires infrastructure metrics.")
    d.panel("Platform JVM threads", [(aggregate(metric(f"jvm_threads_{kind}_threads")), kind) for kind in ("live", "daemon")])
    d.panel("Platform thread states", [(aggregate(metric("jvm_threads_states_threads"), "state"), "{{state}}")])
    d.panel("Deadlocked JVM threads", [(aggregate(metric("jvm_threads_deadlocked_threads")), "total")])
    fd_open = metric("process_files_open_files") + " >= 0"
    fd_max = metric("process_files_max_files") + " > 0"
    d.panel("Open file descriptors", [(aggregate(fd_open), "total")],
            description="Total open descriptors across selected instances. FileDescriptorMetrics is platform-dependent; N/A on unsupported platforms.")
    d.panel("File descriptor utilization", [(aggregate("(" + fd_open + ") / (" + fd_max + ")", op="max"), "maximum per instance")], "percentunit",
            "Highest individual instance utilization; limits are local to each process.")
    d.row("Garbage collection — wall-clock pauses, not GC CPU usage")
    d.panel("Highest instance GC pause fraction", [(aggregate(aggregate(rate("jvm_gc_pause_seconds_sum"), "pod,instance"), op="max"), "maximum per instance")], "percentunit",
            "Maximum of each instance's wall-clock pause time/s, without dividing by CPU count. Event-driven metrics may be absent before first GC.")
    groups = "action,cause"
    d.panel("GC collections/s", [(aggregate(rate("jvm_gc_pause_seconds_count"), groups), "{{action}} {{cause}}")], "ops")
    d.panel("GC pause average / sliding-window max", [(avg("jvm_gc_pause_seconds", groups), "{{action}} {{cause}} average"),
            (aggregate(metric("jvm_gc_pause_seconds_max"), groups, "max"), "{{action}} {{cause}} maximum")], "s",
            "Average is total pause duration / total collection count; maximum is the largest time-window maximum across selected instances.")
    d.panel("Allocation and promotion", [(aggregate(rate(f"jvm_gc_memory_{kind}_bytes_total")), kind) for kind in ("allocated", "promoted")], "Bps")
    d.panel("Classes unloaded/s", [(aggregate(rate("jvm_classes_unloaded_classes_total")), "total")], "ops")
    d.row("Virtual threads — scheduler gauges require Java 24+; default image uses Java 25")
    d.panel("Scheduler parallelism and platform pool", [(aggregate(metric("jvm_threads_virtual_parallelism") + " >= 0"), "total parallelism"),
            (aggregate(metric("jvm_threads_virtual_pool_size_threads") + " >= 0"), "total pool")])
    d.panel("Virtual threads by scheduling status", [(aggregate(metric("jvm_threads_virtual_live_threads") + " >= 0", "scheduling_status"), "{{scheduling_status}}")])
    d.panel("Virtual thread pinning events/s", [(aggregate(rate("jvm_threads_virtual_pinned_seconds_count")), "total")], "ops")
    d.panel("Virtual thread pinned seconds per second", [(aggregate(rate("jvm_threads_virtual_pinned_seconds_sum")), "total")],
            description="Sum of observed pin durations/s across selected instances; concurrent pins can exceed 1s/s. JFR threshold applies.")
    d.panel("Virtual thread submission failures/s", [(aggregate(rate("jvm_threads_virtual_submit_failed_total")), "total")], "ops")

    d.row("Instance details — select pod/instance to inspect", collapsed=True)
    legend = "{{pod}} {{instance}}"
    d.panel("System load and available processors", [(metric("system_load_average_1m"), legend + " load"),
            (metric("system_cpu_count"), legend + " processors")], description="Per-instance runtime values; colocated instances may report the same host resources.")
    d.panel("Platform thread historical peak", [(metric("jvm_threads_peak_threads"), legend)],
            description="Each JVM's historical peak; summing these would not describe a simultaneous service-wide peak.")
    d.panel("Classes loaded", [(metric("jvm_classes_loaded_classes"), legend)])
    d.panel("Process uptime", [(metric("process_uptime_seconds"), legend)], "s")
    d.save("jvm_grafana.json")


if __name__ == "__main__":
    business_dashboard()
    jvm_dashboard()
