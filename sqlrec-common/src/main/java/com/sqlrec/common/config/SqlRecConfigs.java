package com.sqlrec.common.config;

public class SqlRecConfigs {
    public static final ConfigOption<String> DB_URL = new ConfigOption<>(
            "META_DB_URL",
            "jdbc:postgresql://192.168.1.5:30308/sqlrec?sslmode=disable",
            "meta db url",
            null,
            String.class
    );
    public static final ConfigOption<String> DB_USER = new ConfigOption<>(
            "META_DB_USER",
            "sqlrec",
            "meta db user",
            null,
            String.class
    );
    public static final ConfigOption<String> DB_PASSWORD = new ConfigOption<>(
            "META_DB_PASSWORD",
            "abc123456",
            "meta db password",
            null,
            String.class
    );
    public static final ConfigOption<String> DB_DRIVER = new ConfigOption<>(
            "META_DB_DRIVER",
            "org.postgresql.Driver",
            "meta db driver",
            null,
            String.class
    );

    public static final ConfigOption<String> HIVE_METASTORE_URI = new ConfigOption<>(
            "HIVE_METASTORE_URI",
            "thrift://192.168.1.5:32083",
            "hive metastore uri",
            null,
            String.class
    );

    public static final ConfigOption<String> EXECUTE_SET_UGI = new ConfigOption<>(
            "EXECUTE_SET_UGI",
            "false",
            "value of metastore.execute.setugi param",
            null,
            String.class
    );

    public static final ConfigOption<String> FLINK_SQL_GATEWAY_ADDRESS = new ConfigOption<>(
            "FLINK_SQL_GATEWAY_ADDRESS",
            "192.168.1.5",
            "flink sql gateway to proxy",
            null,
            String.class
    );

    public static final ConfigOption<Integer> FLINK_SQL_GATEWAY_PORT = new ConfigOption<>(
            "FLINK_SQL_GATEWAY_PORT",
            30000,
            "flink sql gateway port",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> FLINK_SQL_GATEWAY_CONNECT_TIMEOUT = new ConfigOption<>(
            "FLINK_SQL_GATEWAY_CONNECT_TIMEOUT",
            600000,
            "flink sql gateway connect timeout",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> THRIFT_SERVER_PORT = new ConfigOption<>(
            "THRIFT_SERVER_PORT",
            8000,
            "port of thrift server",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> REST_SERVER_PORT = new ConfigOption<>(
            "REST_SERVER_PORT",
            8001,
            "port of rest server",
            null,
            Integer.class
    );

    public static final ConfigOption<String> DEFAULT_TEST_IP = new ConfigOption<>(
            "DEFAULT_TEST_IP",
            "192.168.1.5",
            "default test ip",
            null,
            String.class
    );

    public static final ConfigOption<String> PARALLELISM_EXEC = new ConfigOption<>(
            "PARALLELISM_EXEC",
            "true",
            "is parallelism exec or not",
            null,
            String.class
    );

    public static final ConfigOption<Integer> FUNCTION_UPDATE_INTERVAL = new ConfigOption<>(
            "FUNCTION_UPDATE_INTERVAL",
            300,
            "function update interval in seconds",
            null,
            Integer.class
    );

    public static final ConfigOption<Integer> SCHEMA_CACHE_EXPIRE = new ConfigOption<>(
            "SCHEMA_CACHE_EXPIRE",
            60,
            "schema cache expire in seconds",
            null,
            Integer.class
    );
    public static final ConfigOption<Boolean> ASYNC_SCHEMA_UPDATE = new ConfigOption<>(
            "ASYNC_SCHEMA_UPDATE",
            true,
            "async schema update or not",
            null,
            Boolean.class
    );
}
