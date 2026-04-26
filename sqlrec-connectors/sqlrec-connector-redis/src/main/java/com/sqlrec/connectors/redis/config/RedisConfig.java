package com.sqlrec.connectors.redis.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;

public class RedisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public String url;
    public String redisMode;
    public String dataStructure;
    public Integer maxListSize;
    public Integer ttl;
    public Integer cacheTtl;
    public Integer maxCacheSize;
    public String database;
    public String tableName;
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
