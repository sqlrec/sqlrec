package com.sqlrec.config;

public class SqlRecConfigs {
    public static final ConfigOption<String> DB_URL = new ConfigOption<>(
            "META_DB_URL",
            "jdbc:mysql://127.0.0.1:30308/sqlrec?allowPublicKeyRetrieval=true&useSSL=false",
            "meta db url"
    );
    public static final ConfigOption<String> DB_USER = new ConfigOption<>(
            "META_DB_USER",
            "sqlrec",
            "meta db user"
    );
    public static final ConfigOption<String> DB_PASSWORD = new ConfigOption<>(
            "META_DB_PASSWORD",
            "abc123456",
            "meta db password"
    );
    public static final ConfigOption<String> DB_DRIVER = new ConfigOption<>(
            "META_DB_DRIVER",
            "com.mysql.cj.jdbc.Driver",
            "meta db driver"
    );

    public static final ConfigOption<String> HIVE_METASTORE_URI = new ConfigOption<>(
            "HIVE_METASTORE_URI",
            "thrift://127.0.0.1:32083",
            "hive metastore uri"
    );
}
