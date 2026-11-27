package com.sqlrec.utils;

import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ThriftRowConversionTest {
    @Test
    void preservesTypedColumnsNullsAndJsonFallbackWithOneEnumeration() {
        JavaTypeFactoryImpl factory = new JavaTypeFactoryImpl();
        var fields = factory.builder()
                .add("text", SqlTypeName.VARCHAR).add("char", SqlTypeName.CHAR)
                .add("tiny", SqlTypeName.TINYINT).add("small", SqlTypeName.SMALLINT)
                .add("int", SqlTypeName.INTEGER).add("long", SqlTypeName.BIGINT)
                .add("float", SqlTypeName.FLOAT).add("double", SqlTypeName.DOUBLE)
                .add("bool", SqlTypeName.BOOLEAN).add("json", SqlTypeName.ANY)
                .build().getFieldList();
        List<Object[]> rows = Arrays.asList(
                new Object[]{"text", "c", -128, 3, 4L, 5, 1.5f, 2.5d, true, List.of(1, 2)},
                new Object[10]);
        AtomicInteger enumerations = new AtomicInteger();
        var source = new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                enumerations.incrementAndGet();
                return Linq4j.asEnumerable(rows).enumerator();
            }
        };

        var result = ThriftUtils.convertObjectArrayToTRowSet(source, fields);
        var columns = result.getColumns();

        assertEquals(1, enumerations.get());
        assertEquals(List.of("text", ""), columns.get(0).getStringVal().getValues());
        assertEquals(List.of("c", ""), columns.get(1).getStringVal().getValues());
        assertEquals(TTypeId.TINYINT_TYPE, ThriftUtils.convertFieldsToTTableSchema(fields)
                .getColumns().get(2).getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(List.of((byte) -128, (byte) 0), columns.get(2).getByteVal().getValues());
        assertEquals(List.of((short) 3, (short) 0), columns.get(3).getI16Val().getValues());
        assertEquals(List.of(4, 0), columns.get(4).getI32Val().getValues());
        assertEquals(List.of(5L, 0L), columns.get(5).getI64Val().getValues());
        assertEquals(List.of(1.5d, 0.0d), columns.get(6).getDoubleVal().getValues());
        assertEquals(List.of(2.5d, 0.0d), columns.get(7).getDoubleVal().getValues());
        assertEquals(List.of(true, false), columns.get(8).getBoolVal().getValues());
        assertEquals(List.of("[1,2]", ""), columns.get(9).getStringVal().getValues());
        assertArrayEquals(new byte[]{2}, columns.get(0).getStringVal().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(2).getByteVal().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(4).getI32Val().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(5).getI64Val().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(6).getDoubleVal().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(8).getBoolVal().getNulls());
        assertArrayEquals(new byte[]{2}, columns.get(9).getStringVal().getNulls());
        assertTrue(result.getRows().isEmpty());
        assertTrue(result.isSetStartRowOffset());
    }

    @Test
    void convertsCellsBeforeTheSourceReusesItsBuffers() {
        var factory = new JavaTypeFactoryImpl();
        var fields = factory.builder().add("id", SqlTypeName.INTEGER).add("label", SqlTypeName.VARCHAR)
                .add("items", SqlTypeName.ANY)
                .build().getFieldList();
        int[] items = new int[1];
        Object[] buffer = {null, null, items};
        var source = Linq4j.asEnumerable(List.of(1, 2)).select(id -> {
            buffer[0] = id;
            buffer[1] = "row " + id;
            items[0] = id;
            return buffer;
        });

        var columns = ThriftUtils.convertObjectArrayToTRowSet(source, fields).getColumns();

        assertEquals(List.of(1, 2), columns.get(0).getI32Val().getValues());
        assertEquals(List.of("row 1", "row 2"), columns.get(1).getStringVal().getValues());
        assertEquals(List.of("[1]", "[2]"), columns.get(2).getStringVal().getValues());
    }

    @Test
    void realColumnsUseFloatingPointMetadataAndValues() {
        var factory = new JavaTypeFactoryImpl();
        var fields = factory.builder().add("value", SqlTypeName.REAL).build().getFieldList();
        var schema = ThriftUtils.convertFieldsToTTableSchema(fields);
        var rows = Linq4j.asEnumerable(List.of(new Object[]{1.5f}, new Object[]{null}));
        var column = ThriftUtils.convertObjectArrayToTRowSet(rows, fields).getColumns().getFirst().getDoubleVal();

        assertEquals(TTypeId.FLOAT_TYPE,
                schema.getColumns().getFirst().getTypeDesc().getTypes().getFirst().getPrimitiveEntry().getType());
        assertEquals(List.of(1.5d, 0.0d), column.getValues());
        assertArrayEquals(new byte[]{2}, column.getNulls());
    }
}
