package com.sqlrec.common.config;

public class Consts {
    public static String DEFAULT_SCHEMA_NAME = "default";

    public static String CHECKPOINT_TYPE_ORIGIN = "origin";
    public static String CHECKPOINT_TYPE_EXPORT = "export";

    public static String CHECKPOINT_STATUS_CREATED = "created";
    public static String CHECKPOINT_STATUS_SUCCEEDED = "succeeded";
    public static String CHECKPOINT_STATUS_FAILED = "failed";

    public static String METRICS_NODE_EXEC_TIME = "node.exec.time";
    public static String METRICS_NODE_EXEC_COUNT = "node.exec.count";
    public static String METRICS_NODE_DATA_SIZE = "node.data.size";
}
