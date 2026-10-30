package com.sqlrec.utils;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.calcite.linq4j.Linq4j;
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

        var result = ThriftUtils.getValueList(Linq4j.asEnumerable(rows), 0, Integer.class);

        assertArrayEquals(new byte[]{(byte) 0x81, 0x01}, result.getKey());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 0, 0, 9), result.getValue());
        assertNull(rows.get(0)[0]);
        assertEquals(1L, rows.get(1)[0]);
    }

    @Test
    @SilenceLoggers(ThriftUtils.class)
    public void testThriftValuesForMissingInputAndFailedCast() {
        var empty = ThriftUtils.getValueList(null, 0, String.class);
        assertArrayEquals(new byte[0], empty.getKey());
        assertTrue(empty.getValue().isEmpty());

        var result = ThriftUtils.getValueList(
                Linq4j.asEnumerable(List.of(new Object[]{"value"}, new Object[]{42})),
                0, String.class);
        assertArrayEquals(new byte[]{2}, result.getKey());
        assertEquals(List.of("value", ""), result.getValue());
    }

}
