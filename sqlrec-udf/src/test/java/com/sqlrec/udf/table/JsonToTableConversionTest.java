package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonToTableConversionTest {
    private final JsonToTableFunction function = new JsonToTableFunction();

    @Test
    void preservesFieldOrderAndUsesFirstNonNullValueForColumnTypes() {
        CacheTable table = function.evaluate(
                "[{\"missing\":null,\"number\":1,\"flag\":true,\"text\":\"first\"},"
                        + "{\"number\":\"2\",\"flag\":1,\"text\":{\"name\":\"nested\"},"
                        + "\"late\":false,\"missing\":true}]");

        assertEquals(Arrays.asList("missing", "number", "flag", "text", "late"),
                table.getDataFields().stream().map(RelDataTypeField::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList(SqlTypeName.BOOLEAN, SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN,
                        SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN),
                table.getDataFields().stream().map(field -> field.getType().getSqlTypeName())
                        .collect(Collectors.toList()));
        List<Object[]> rows = table.scan(null).toList();
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{null, 1.0d, true, "first", null}, rows.get(0));
        assertArrayEquals(new Object[]{true, null, null, "{\"name\":\"nested\"}", false}, rows.get(1));
    }

    @Test
    void arrayTypesUseFirstPrimitiveAndKeepIncompatibleElementsNull() {
        CacheTable table = function.evaluate(
                "{\"numbers\":[null,{},[1],2,\"3\",true],"
                        + "\"booleans\":[false,1,\"true\",{},null],"
                        + "\"strings\":[\"first\",2,true,{\"x\":1},[3],null],"
                        + "\"empty\":[],\"nestedOnly\":[{},[1],null],\"nulls\":[null,null]}");

        assertEquals(Arrays.asList(SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR,
                        SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR),
                table.getDataFields().stream()
                        .map(field -> field.getType().getComponentType().getSqlTypeName())
                        .collect(Collectors.toList()));
        assertArrayEquals(new Object[]{
                Arrays.asList(null, null, null, 2.0d, null, null),
                Arrays.asList(false, null, null, null, null),
                Arrays.asList("first", "2", "true", "{\"x\":1}", "[3]", null),
                Collections.emptyList(), Arrays.asList("{}", "[1]", null), Arrays.asList(null, null)
        }, table.scan(null).toList().get(0));
    }

    @Test
    void keepsArrayColumnTypeFromFirstValueAndSkipsNonObjectRows() {
        CacheTable table = function.evaluate(
                "[null,1,[],{\"array\":[],\"absent\":null},{\"array\":[1,true,{}]},"
                        + "{\"array\":\"scalar\"}]");

        List<Object[]> rows = table.scan(null).toList();
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{Collections.emptyList(), null}, rows.get(0));
        assertArrayEquals(new Object[]{Arrays.asList("1", "true", "{}"), null}, rows.get(1));
        assertArrayEquals(new Object[]{null, null}, rows.get(2));
        assertEquals(SqlTypeName.VARCHAR,
                table.getDataFields().get(0).getType().getComponentType().getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, table.getDataFields().get(1).getType().getSqlTypeName());
    }
}
