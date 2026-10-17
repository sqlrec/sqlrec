package com.sqlrec.common.config;

public class Consts {
    public static final String DEFAULT_SCHEMA_NAME = "default";
    // Version-pinned HiveCatalog compatibility baseline; these are not deployment settings.
    public static final String HIVE_CATALOG_NAME = "hive";
    public static final String HIVE_CLIENT_VERSION = "3.1.3";

    public static final String CHECKPOINT_TYPE_ORIGIN = "origin";
    public static final String CHECKPOINT_TYPE_EXPORT = "export";

    public static final String CHECKPOINT_STATUS_CREATED = "created";
    public static final String CHECKPOINT_STATUS_SUCCEEDED = "succeeded";
    public static final String CHECKPOINT_STATUS_FAILED = "failed";

    public static final String METRICS_NODE_EXEC_DURATION = "sqlrec.node.exec.duration";
    public static final String METRICS_NODE_ROWS = "sqlrec.node.rows";
    public static final String METRICS_NODE_CANCELLED = "sqlrec.node.cancelled";
    public static final String METRICS_CACHE_TABLE_IGNORE_EXCEPTION = "sqlrec.cache.table.ignore.exception";
    public static final String METRICS_IF_CACHE_BRANCH = "sqlrec.if.cache.branch";
    public static final String METRICS_IF_TIMEIN_OUTCOME = "sqlrec.if.timein.outcome";
    public static final String METRICS_TABLE_SCAN_DURATION = "sqlrec.table.scan.duration";
    public static final String METRICS_TABLE_SCAN_ROWS = "sqlrec.table.scan.rows";
    public static final String METRICS_TABLE_VECTOR_SEARCH_DURATION = "sqlrec.table.vector.search.duration";
    public static final String METRICS_TABLE_VECTOR_SEARCH_ROWS = "sqlrec.table.vector.search.rows";
    public static final String METRICS_TABLE_GET_BY_PRIMARY_KEY_DURATION = "sqlrec.table.get.by.primary.key.duration";
    public static final String METRICS_TABLE_GET_BY_PRIMARY_KEY_KEYS = "sqlrec.table.get.by.primary.key.keys";
    public static final String METRICS_TABLE_WRITE_DURATION = "sqlrec.table.write.duration";
    public static final String METRICS_HTTP_REQUEST_DURATION = "sqlrec.http.request.duration";
    public static final String METRICS_API_EXECUTION_DURATION = "sqlrec.api.execution.duration";
    public static final String METRICS_SQL_EXECUTION_DURATION = "sqlrec.sql.execution.duration";
    public static final String METRICS_FUNCTION_UPDATE_DURATION = "sqlrec.function.update.duration";
    public static final String METRICS_REGISTRATION_FAILURES = "sqlrec.metrics.registration.failures";
    public static final String METRICS_FEATURE_OBSERVED = "sqlrec.feature.observed";
    public static final String METRICS_FEATURE_NON_MISSING = "sqlrec.feature.non.missing";
    public static final String METRICS_FEATURE_EMPTY_BATCHES = "sqlrec.feature.empty.batches";
    public static final String METRICS_SESSION_OPEN_COUNT = "sqlrec.session.open.count";
    public static final String METRICS_SESSION_CLOSE_COUNT = "sqlrec.session.close.count";
    public static final String METRICS_SESSION_ACTIVE_COUNT = "sqlrec.session.active.count";
    public static final String METRICS_OPERATION_OPEN_COUNT = "sqlrec.operation.open.count";
    public static final String METRICS_OPERATION_CLOSE_COUNT = "sqlrec.operation.close.count";
    public static final String METRICS_OPERATION_ACTIVE_COUNT = "sqlrec.operation.active.count";

    public static final String LOG_ID = "log_id";
}
