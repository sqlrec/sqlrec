package com.sqlrec.connectors.redis.config;

import com.sqlrec.common.utils.FieldSchema;

import java.util.List;

public class RedisConfig {
    public String url;
    public String redisMode;
    public String dataStructure;
    public Integer ttl;
    public Integer cacheTtl;
    public Integer maxCacheSize;
    public String database;
    public String tableName;
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
