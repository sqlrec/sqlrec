package com.sqlrec.common.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable execution metadata shared by HMS and Flink connector factories. */
public final class ConnectorTableMetadata {
    private final String database;
    private final String tableName;
    private final Map<String, String> options;
    private final List<FieldSchema> fieldSchemas;
    private final String primaryKey;
    private final int primaryKeyIndex;

    public ConnectorTableMetadata(
            String database, String tableName, Map<String, String> options,
            List<FieldSchema> fieldSchemas, String primaryKey, int primaryKeyIndex) {
        this.database = database;
        this.tableName = tableName;
        this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
        this.fieldSchemas = copyFields(fieldSchemas);
        this.primaryKey = primaryKey;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    /** Returns an independent list because backend configurations use mutable FieldSchema values. */
    public List<FieldSchema> getFieldSchemas() {
        return copyFields(fieldSchemas);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    private static List<FieldSchema> copyFields(List<FieldSchema> fields) {
        List<FieldSchema> copy = new ArrayList<>(fields.size());
        for (FieldSchema field : fields) {
            copy.add(new FieldSchema(field.getName(), field.getType()));
        }
        return copy;
    }
}
