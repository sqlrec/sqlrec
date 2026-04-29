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
    
    public Integer batchSize = 1024;
    public Integer poolMaxIdlePerKey = 10;
    public Integer poolMaxTotalPerKey = 100;
    public Integer poolMaxTotal = 100;
    public Long poolMaxBlockWaitDuration = 5L;
    public Long poolMinEvictableIdleDuration = 10L;
    public Long flushInterval = 5L;
}
