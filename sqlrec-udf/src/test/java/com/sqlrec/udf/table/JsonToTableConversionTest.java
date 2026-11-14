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
    void preservesLargeIntegerValuesAndArrayElementsExactly() {
        CacheTable table = function.evaluate(
                "{\"id\":9007199254740993,\"ids\":[-9223372036854775808,9223372036854775807],"
                        + "\"matrix\":[[9007199254740993]]}");
        assertEquals(SqlTypeName.BIGINT, table.getDataFields().get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT,
                table.getDataFields().get(1).getType().getComponentType().getSqlTypeName());
        assertArrayEquals(new Object[]{9007199254740993L, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                List.of(List.of(9007199254740993L))}, table.scan(null).toList().get(0));
    }

    @Test
    void widensMixedNumericRowsAndArraysBeforeDecoding() {
        CacheTable table = function.evaluate(
                "[{\"value\":1,\"values\":[1,0.5],\"matrix\":[[1],[2.5]]},"
                        + "{\"value\":0.75,\"values\":[2],\"matrix\":[[3]]}]");
        assertEquals(SqlTypeName.DOUBLE, table.getDataFields().get(0).getType().getSqlTypeName());
        List<Object[]> rows = table.scan(null).toList();
        assertArrayEquals(new Object[]{1.0d, List.of(1.0d, 0.5d), List.of(List.of(1.0d), List.of(2.5d))}, rows.get(0));
        assertArrayEquals(new Object[]{0.75d, List.of(2.0d), List.of(List.of(3.0d))}, rows.get(1));
    }

    @Test
    void preservesFieldOrderAndUsesFirstNonNullValueForColumnTypes() {
        CacheTable table = function.evaluate(
                "[{\"missing\":null,\"number\":1,\"flag\":true,\"text\":\"first\"},"
                        + "{\"number\":\"2\",\"flag\":1,\"text\":{\"name\":\"nested\"},"
                        + "\"late\":false,\"missing\":true}]");

        assertEquals(Arrays.asList("missing", "number", "flag", "text", "late"),
                table.getDataFields().stream().map(RelDataTypeField::getName).collect(Collectors.toList()));
        assertEquals(Arrays.asList(SqlTypeName.BOOLEAN, SqlTypeName.BIGINT, SqlTypeName.BOOLEAN,
                        SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN),
                table.getDataFields().stream().map(field -> field.getType().getSqlTypeName())
                        .collect(Collectors.toList()));
        List<Object[]> rows = table.scan(null).toList();
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{null, 1L, true, "first", null}, rows.get(0));
        assertArrayEquals(new Object[]{true, 2L, false, "{\"name\":\"nested\"}", false}, rows.get(1));
    }

    @Test
    void arrayTypesUseFirstPrimitiveAndKeepIncompatibleElementsNull() {
        CacheTable table = function.evaluate(
                "{\"numbers\":[null,{},[1],2,\"3\",true],"
                        + "\"booleans\":[false,1,\"true\",{},null],"
                        + "\"strings\":[\"first\",2,true,{\"x\":1},[3],null],"
                        + "\"empty\":[],\"nestedOnly\":[{},[1],null],\"nulls\":[null,null]}");

        assertEquals(Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR,
                        SqlTypeName.VARCHAR, SqlTypeName.ARRAY, SqlTypeName.VARCHAR),
                table.getDataFields().stream()
                        .map(field -> field.getType().getComponentType().getSqlTypeName())
                        .collect(Collectors.toList()));
        assertArrayEquals(new Object[]{
                Arrays.asList(null, null, null, 2L, 3L, null),
                Arrays.asList(false, false, true, null, null),
                Arrays.asList("first", "2", "true", "{\"x\":1}", "[3]", null),
                Collections.emptyList(), Arrays.asList(null, List.of(1L), null), Arrays.asList(null, null)
        }, table.scan(null).toList().get(0));
    }

    @Test
    void infersArrayElementsAcrossRowsAndSkipsNonObjectRows() {
        CacheTable table = function.evaluate(
                "[null,1,[],{\"array\":[],\"absent\":null},{\"array\":[1,true,{}]},"
                        + "{\"array\":\"scalar\"}]");

        List<Object[]> rows = table.scan(null).toList();
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{Collections.emptyList(), null}, rows.get(0));
        assertArrayEquals(new Object[]{Arrays.asList(1L, null, null), null}, rows.get(1));
        assertArrayEquals(new Object[]{null, null}, rows.get(2));
        assertEquals(SqlTypeName.BIGINT,
                table.getDataFields().get(0).getType().getComponentType().getSqlTypeName());
        assertEquals(SqlTypeName.VARCHAR, table.getDataFields().get(1).getType().getSqlTypeName());
    }
}
