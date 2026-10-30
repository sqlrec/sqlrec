package com.sqlrec.common.utils;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HiveTableModificationTimeTest {
    @Test
    void hmsTimestampsRetainTheirSecondsBasedContract() {
        Table table = new Table();
        table.putToParameters("transient_lastDdlTime", "1700000000");
        assertEquals(1700000000000L, HiveTableUtils.getTableModificationTime(table));
    }

    @Test
    void missingAndInvalidTimestampsReturnZero() {
        Table table = new Table();
        assertEquals(0, HiveTableUtils.getTableModificationTime(table));
        table.putToParameters("transient_lastDdlTime", "invalid");
        assertEquals(0, HiveTableUtils.getTableModificationTime(table));
    }
}
