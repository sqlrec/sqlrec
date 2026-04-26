package com.sqlrec.connectors.milvus.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;

public class MilvusConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public String url;
    public String token;
    public String database;
    public String collection;
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
