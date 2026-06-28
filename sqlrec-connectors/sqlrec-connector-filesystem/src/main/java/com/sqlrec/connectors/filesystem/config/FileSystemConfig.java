package com.sqlrec.connectors.filesystem.config;

import com.sqlrec.common.schema.FieldSchema;

import java.io.Serializable;
import java.util.List;

public class FileSystemConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public String path;
    public String format;

    // common
    public List<FieldSchema> fieldSchemas;
    public String primaryKey;
    public Integer primaryKeyIndex;
}
