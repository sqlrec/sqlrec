package com.sqlrec.compiler;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlCommandCompatibilityTest {
    @Test
    void setValuesSurviveParsingAndCanonicalRendering() throws Exception {
        for (String sql : List.of(
                "SET table.sql-dialect=default;",
                "-- before\nSET\tparallelism.default=2 -- after",
                "SET key='it''s a;b=c'",
                "SET normalizer=method=minmax,min=0,max=100",
                "SET key=;")) {
            SqlSet original = assertInstanceOf(SqlSet.class, CompileManager.parseSql(sql));
            SqlSet reparsed = assertInstanceOf(SqlSet.class,
                    CompileManager.parseSql(CompileManager.getSqlStr(original)));
            assertEquals(original.getKeyString(), reparsed.getKeyString(), sql);
            assertEquals(original.getValueString(), reparsed.getValueString(), sql);
        }
    }

    @Test
    void defaultDatabaseCompatibilitySurvivesCanonicalRendering() throws Exception {
        for (String sql : List.of("USE DEFAULT;", "SHOW TABLES IN default;", "SHOW TABLES FROM DEFAULT")) {
            SqlNode original = CompileManager.parseSql(sql);
            SqlNode reparsed = CompileManager.parseSql(CompileManager.getSqlStr(original));
            assertEquals(original.getClass(), reparsed.getClass());
            if (reparsed instanceof SqlUseDatabase use) {
                assertArrayEquals(new String[]{"default"}, use.fullDatabaseName());
            } else {
                assertArrayEquals(new String[]{"default"}, ((SqlShowTables) reparsed).fullDatabaseName());
            }
        }
    }

    @Test
    void malformedCommandsAreRejectedAtTheirOriginalPositions() {
        SqlParseException error = assertThrows(SqlParseException.class,
                () -> CompileManager.parseSql("-- before\nSET key='value' extra"));
        assertEquals(2, error.getPos().getLineNum());
        assertEquals(17, error.getPos().getColumnNum());
        assertThrows(SqlParseException.class, () -> CompileManager.parseSql("usedefault"));
        assertThrows(SqlParseException.class, () -> CompileManager.parseSql("SET key=value; DROP TABLE t"));
    }
}
