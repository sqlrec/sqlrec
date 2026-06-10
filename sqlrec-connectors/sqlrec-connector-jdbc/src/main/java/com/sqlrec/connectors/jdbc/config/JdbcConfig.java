package com.sqlrec.connectors.jdbc.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JdbcConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // basic connection
    public String url;
    public String tableName;
    public String username;
    public String password;
    public String driver;
    public String schema;

    // cache
    public Integer maxCacheSize;
    public Integer cacheTtl;

    // connection pool
    public Integer connectionPoolSize;
    public Integer connectionPoolMinIdle;
    public Long connectionPoolIdleTimeout;
    public Long connectionPoolMaxLifetime;
    public Long connectionPoolConnectionTimeout;
    public Long connectionPoolValidationTimeout;
    public Long connectionPoolKeepaliveTime;
    public String connectionPoolName;

    // jdbc custom properties
    public Map<String, String> jdbcProperties;

    // common
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
    public String database;
}
