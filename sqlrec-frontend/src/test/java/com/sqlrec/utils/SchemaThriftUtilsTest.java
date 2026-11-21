package com.sqlrec.utils;

import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaThriftUtilsTest {

    @Test
    public void testThriftValuesPreserveNullBitmapAcrossBytes() {
        List<Object[]> rows = List.of(
                new Object[]{null}, new Object[]{1L}, new Object[]{2}, new Object[]{3},
                new Object[]{4}, new Object[]{5}, new Object[]{6}, new Object[]{null},
                new Object[]{null}, new Object[]{9});

        var fields = List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        var column = ThriftUtils.convertObjectArrayToTRowSet(Linq4j.asEnumerable(rows), fields)
                .getColumns().getFirst().getI32Val();

        assertArrayEquals(new byte[]{(byte) 0x81, 0x01}, column.getNulls());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 0, 0, 9), column.getValues());
        assertNull(rows.get(0)[0]);
        assertEquals(1L, rows.get(1)[0]);
    }

    @Test
    @SilenceLoggers(ThriftUtils.class)
    public void testThriftValuesForMissingInputAndFailedCast() {
        var fields = DataTypeUtils.getStringTypeField("name");
        var empty = ThriftUtils.convertObjectArrayToTRowSet(null, fields)
                .getColumns().getFirst().getStringVal();
        assertArrayEquals(new byte[0], empty.getNulls());
        assertTrue(empty.getValues().isEmpty());

        var column = ThriftUtils.convertObjectArrayToTRowSet(
                Linq4j.asEnumerable(List.of(new Object[]{"value"}, new Object[]{42})), fields)
                .getColumns().getFirst().getStringVal();
        assertArrayEquals(new byte[]{2}, column.getNulls());
        assertEquals(List.of("value", ""), column.getValues());
    }

}
