package com.sqlrec.utils;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.sql.parser.SqlCreateModel;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaUtilsTest {

    @Test
    public void testRemoveQuotesWithSingleQuotes() {
        String input = "'test_value'";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithDoubleQuotes() {
        String input = "\"test_value\"";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithNoQuotes() {
        String input = "test_value";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value", result);
    }

    @Test
    public void testRemoveQuotesWithNull() {
        String result = SchemaUtils.removeQuotes(null);
        assertNull(result);
    }

    @Test
    public void testRemoveQuotesWithOnlyStartQuote() {
        String input = "'test_value";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("'test_value", result);
    }

    @Test
    public void testRemoveQuotesWithOnlyEndQuote() {
        String input = "test_value'";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("test_value'", result);
    }

    @Test
    public void testRemoveQuotesWithEmptyString() {
        String input = "";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("", result);
    }

    @Test
    public void testRemoveQuotesWithQuotedEmptyString() {
        String input = "''";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("", result);
    }

    @Test
    public void testRemoveQuotesWithMismatchedQuotes() {
        String input = "'test_value\"";
        String result = SchemaUtils.removeQuotes(input);
        assertEquals("'test_value\"", result);
    }

    @Test
    public void testGetValueOfStringLiteralWithNull() {
        String result = SchemaUtils.getValueOfStringLiteral(null);
        assertNull(result);
    }

    @Test
    public void testConvertFieldListWithNull() {
        var result = SchemaUtils.convertFieldList(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testConvertPropertyListWithNull() {
        var result = SchemaUtils.convertPropertyList(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void readsSqlRecAndFlinkOptionsFromTheirStringLiterals() throws Exception {
        var model = (SqlCreateModel) CompileManager.parseSql("""
                CREATE MODEL sample (x FLOAT) WITH (
                    'owner''s title' = 'It''s '
                                       '"ready"',
                    'binary' = X'AB'
                )
                """);
        assertEquals(Map.of("owner's title", "It's \"ready\"", "binary", "X'AB'"),
                SchemaUtils.convertPropertyList(model.getPropertyList()));

        var pos = SqlParserPos.ZERO;
        var options = new SqlNodeList(List.of(
                new SqlTableOption(SqlLiteral.createCharString("table's title", pos),
                        SqlLiteral.createCharString("", pos), pos)), pos);

        assertEquals(Map.of("table's title", ""), SchemaUtils.convertPropertyList(options));
    }

    @Test
    public void testGetTableObjWithNullSchema() {
        var result = SchemaUtils.getTableObj(null, "test_table");
        assertNull(result);
    }

    @Test
    public void testGetTableObjWithNullTableName() {
        var schema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false);
        var result = SchemaUtils.getTableObj(schema, null);
        assertNull(result);
    }

    @Test
    public void testGetTableObjWithNonExistentTable() {
        var schema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false);
        var result = SchemaUtils.getTableObj(schema, "non_existent_table");
        assertNull(result);
    }
}
