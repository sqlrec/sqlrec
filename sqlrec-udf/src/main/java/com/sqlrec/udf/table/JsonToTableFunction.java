package com.sqlrec.udf.table;

import com.google.gson.JsonObject;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.SchemaInference;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;

public class JsonToTableFunction {
    public CacheTable evaluate(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            throw new IllegalArgumentException("json string is empty");
        }
        List<JsonObject> objects = JsonRows.readObjects(jsonString);
        if (objects.isEmpty()) {
            throw new IllegalArgumentException("no json objects found in input");
        }
        List<FieldSchema> fields = SchemaInference.inferJsonFields(objects);
        List<RelDataTypeField> dataFields = DataTypeUtils.getRelDataType(
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), fields).getFieldList();
        List<Object[]> rows = JsonRows.decodeRows(objects, fields, JsonRows.Decoding.MATCH_SCHEMA);
        return new CacheTable("output", Linq4j.asEnumerable(rows), dataFields);
    }
}
