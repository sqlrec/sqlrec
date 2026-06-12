package com.sqlrec.connectors.mongodb.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;

public class MongoConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // basic connection
    public String uri;
    public String database;
    public String collection;

    // cache
    public Integer maxCacheSize;
    public Integer cacheTtl;

    // common
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
