package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.utils.FieldSchema;

import java.util.List;

public class MilvusConfig {
    public String url;
    public String token;
    public String database;
    public String collection;
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
