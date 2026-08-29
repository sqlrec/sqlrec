package com.sqlrec.compiler;

import com.sqlrec.sql.parser.SqlReturn;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlReturnParserTest {

    @Test
    void parsesEmptyReturn() throws Exception {
        SqlReturn sqlReturn = parse("RETURN");

        assertNull(sqlReturn.getTableName());
        assertNull(sqlReturn.getSelect());
        assertNull(sqlReturn.getCallSqlFunction());
        assertTrue(sqlReturn.getOperandList().isEmpty());
        assertEquals("RETURN", plainSql(sqlReturn));
    }

    @Test
    void parsesTableReturn() throws Exception {
        SqlReturn sqlReturn = parse("RETURN result_table");

        assertEquals("result_table", sqlReturn.getTableName().getSimple());
        assertNull(sqlReturn.getSelect());
        assertNull(sqlReturn.getCallSqlFunction());
        assertEquals(1, sqlReturn.getOperandList().size());
        assertEquals("RETURNRESULT_TABLE", plainSql(sqlReturn));
    }

    @Test
    void parsesSelectReturn() throws Exception {
        SqlReturn sqlReturn = parse("RETURN SELECT id FROM source_table WHERE id > 1");

        assertNull(sqlReturn.getTableName());
        assertNotNull(sqlReturn.getSelect());
        assertNull(sqlReturn.getCallSqlFunction());
        assertEquals(1, sqlReturn.getOperandList().size());
        assertEquals(
                "RETURNSELECTIDFROMSOURCE_TABLEWHEREID>1",
                plainSql(sqlReturn)
        );
    }

    @Test
    void parsesCallReturnIncludingCallOptions() throws Exception {
        SqlReturn sqlReturn = parse(
                "RETURN CALL child(input_table) LIKE FUNCTION 'shape' PARTITION BY input_table SIZE 10"
        );

        assertNull(sqlReturn.getTableName());
        assertNull(sqlReturn.getSelect());
        assertNotNull(sqlReturn.getCallSqlFunction());
        assertFalse(sqlReturn.getCallSqlFunction().isAsync());
        assertEquals(1, sqlReturn.getOperandList().size());
        assertTrue(plainSql(sqlReturn).startsWith("RETURNCALLCHILD(INPUT_TABLE)"));
    }

    @Test
    void parsesAsyncCallSoCompilerCanRejectItExplicitly() throws Exception {
        SqlReturn sqlReturn = parse("RETURN CALL child(input_table) ASYNC");

        assertTrue(sqlReturn.getCallSqlFunction().isAsync());
    }

    @Test
    void constructorRejectsMultipleResultSources() {
        assertThrows(IllegalArgumentException.class, () -> new SqlReturn(
                SqlParserPos.ZERO,
                new SqlIdentifier("result_table", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                null
        ));
    }

    private static SqlReturn parse(String sql) throws Exception {
        SqlNode node = CompileManager.parseFlinkSql(sql);
        return assertInstanceOf(SqlReturn.class, node);
    }

    private static String plainSql(SqlNode node) {
        return SqlParseTest.getPlainSql(CompileManager.getSqlStr(node));
    }
}
