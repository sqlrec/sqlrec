package com.sqlrec.common.config;

public class Consts {
    public static String DEFAULT_SCHEMA_NAME = "default";

    public static String CHECKPOINT_TYPE_ORIGIN = "origin";
    public static String CHECKPOINT_TYPE_EXPORT = "export";

    public static String CHECKPOINT_STATUS_CREATED = "created";
    public static String CHECKPOINT_STATUS_SUCCEEDED = "succeeded";
    public static String CHECKPOINT_STATUS_FAILED = "failed";

    public static String METRICS_NODE_EXEC_DURATION = "sqlrec.node.exec.duration";
    public static String METRICS_NODE_DATA_SIZE = "sqlrec.node.data.size";
    public static String METRICS_CACHE_TABLE_IGNORE_EXCEPTION = "sqlrec.cache.table.ignore.exception";
    public static String METRICS_IF_CACHE_BRANCH = "sqlrec.if.cache.branch";
    public static String METRICS_IF_CACHE_TIMEOUT = "sqlrec.if.cache.timeout";
    public static String METRICS_IF_CACHE_EXCEPTION_FALLBACK = "sqlrec.if.cache.exception.fallback";
    public static String METRICS_TABLE_SCAN_DURATION = "sqlrec.table.scan.duration";
    public static String METRICS_TABLE_SCAN_DATA_SIZE = "sqlrec.table.scan.data.size";
    public static String METRICS_TABLE_VECTOR_SEARCH_DURATION = "sqlrec.table.vector.search.duration";
    public static String METRICS_TABLE_VECTOR_SEARCH_DATA_SIZE = "sqlrec.table.vector.search.data.size";
    public static String METRICS_TABLE_GET_BY_PRIMARY_KEY_DURATION = "sqlrec.table.get.by.primary.key.duration";
    public static String METRICS_TABLE_GET_BY_PRIMARY_KEY_DATA_SIZE = "sqlrec.table.get.by.primary.key.data.size";
    public static String METRICS_TABLE_CACHE_HIT_COUNT = "sqlrec.table.cache.hit.count";
    public static String METRICS_TABLE_CACHE_DATA_SIZE = "sqlrec.table.cache.data.size";
    public static String METRICS_TABLE_COLLECTION_ADD_DURATION = "sqlrec.table.collection.add.duration";
    public static String METRICS_TABLE_COLLECTION_REMOVE_DURATION = "sqlrec.table.collection.remove.duration";
    public static String METRICS_HTTP_REQUEST_DURATION = "sqlrec.http.request.duration";
    public static String METRICS_HTTP_REQUEST_COUNT = "sqlrec.http.request.count";
    public static String METRICS_FUNCTION_UPDATE_DURATION = "sqlrec.function.update.duration";
    public static String METRICS_FUNCTION_UPDATE_COUNT = "sqlrec.function.update.count";
    public static String METRICS_SESSION_OPEN_COUNT = "sqlrec.session.open.count";
    public static String METRICS_SESSION_CLOSE_COUNT = "sqlrec.session.close.count";
    public static String METRICS_SESSION_ACTIVE_COUNT = "sqlrec.session.active.count";
    public static String METRICS_OPERATION_OPEN_COUNT = "sqlrec.operation.open.count";
    public static String METRICS_OPERATION_CLOSE_COUNT = "sqlrec.operation.close.count";
    public static String METRICS_OPERATION_ACTIVE_COUNT = "sqlrec.operation.active.count";
}
