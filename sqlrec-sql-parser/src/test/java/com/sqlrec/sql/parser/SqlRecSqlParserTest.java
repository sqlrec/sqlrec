package com.sqlrec.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlRecSqlParserTest {
    @Test
    void parsesStandardSqlWithDependencyFlinkParser() throws Exception {
        SqlNode node = SqlRecSqlParser.parse(
                "CREATE TABLE source_table(id BIGINT) WITH ('connector'='values')");

        assertInstanceOf(SqlCreateTable.class, node);
        assertEquals("org.apache.flink.sql.parser.ddl.SqlCreateTable", node.getClass().getName());
    }

    @Test
    void parsesSqlRecModelWithoutFlinkGrammarHelpers() throws Exception {
        SqlCreateModel model = assertInstanceOf(
                SqlCreateModel.class,
                SqlRecSqlParser.parse("""
                        CREATE MODEL rank_model(
                          id BIGINT,
                          genres ARRAY<STRING>
                        ) WITH ('model'='wide_and_deep')
                        """));

        assertEquals(2, model.getFieldList().size());
        assertInstanceOf(SqlRecColumnDeclaration.class, model.getFieldList().get(0));
        SqlRecColumnDeclaration collectionColumn = assertInstanceOf(
                SqlRecColumnDeclaration.class,
                model.getFieldList().get(1));
        assertInstanceOf(
                SqlRecCollectionTypeNameSpec.class,
                collectionColumn.getDataType().getTypeNameSpec());
        assertInstanceOf(SqlRecOption.class, model.getPropertyList().get(0));
    }

    @Test
    void keepsFlinkSetAvailableInsideSqlRecIf() throws Exception {
        SqlIfCache conditional = assertInstanceOf(
                SqlIfCache.class,
                SqlRecSqlParser.parse("IF (SELECT TRUE) THEN (SET 'key'='value')"));

        assertInstanceOf(SqlSet.class, conditional.getThenClause());
    }

    @Test
    void prefersSqlRecCallForAmbiguousProcedureSyntax() throws Exception {
        SqlNode node = SqlRecSqlParser.parse("CALL rank_function(input_table)");

        assertInstanceOf(SqlCallSqlFunction.class, node);
        assertInstanceOf(SqlRecStatement.class, node);
    }

    @Test
    void keepsFlinkProcedureCallWhenSqlRecCallGrammarDoesNotMatch() throws Exception {
        SqlNode node = SqlRecSqlParser.parse("CALL standard_proc(1 + 2)");

        assertEquals(org.apache.calcite.sql.SqlKind.PROCEDURE_CALL, node.getKind());
    }

    @Test
    void exposesRealOperands() throws Exception {
        SqlCache cache = assertInstanceOf(
                SqlCache.class,
                SqlRecSqlParser.parse("CACHE TABLE cached_output AS SELECT 1"));

        assertEquals(2, cache.getOperandList().size());
        assertTrue(cache.getOperandList().contains(cache.getTableName()));
        assertTrue(cache.getOperandList().contains(cache.getSelect()));
    }
}
