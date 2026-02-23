package com.sqlrec.model.common;

import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

public class ModelConfig {
    public String modelName;
    public List<FieldSchema> fieldSchemas;
    public Map<String, String> params;
}
