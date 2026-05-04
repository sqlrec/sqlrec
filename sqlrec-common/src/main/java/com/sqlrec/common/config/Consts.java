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
}
