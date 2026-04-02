package com.sqlrec.common.model;

import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

public class ModelConfig {
    public String modelName;
    public List<FieldSchema> inputFields;
    public Map<String, String> params;
    public String ddl;
    public String path;
}
