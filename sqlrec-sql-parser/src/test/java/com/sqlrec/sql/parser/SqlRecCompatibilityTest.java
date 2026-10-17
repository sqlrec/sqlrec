package com.sqlrec.sql.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SqlRecCompatibilityTest {
    private record Setting(String sql, String key, String value) {}

    @TestFactory
    Stream<DynamicTest> setSpellingsProduceStringLiteralValues() {
        return Stream.of(
                new Setting("SET table.sql-dialect = default", "table.sql-dialect", "default"),
                new Setting("set param=test", "param", "test"),
                new Setting("SeT Mixed.Case-Key=VaLuE", "Mixed.Case-Key", "VaLuE"),
                new Setting("SET\tkey=value", "key", "value"),
                new Setting("SET\nkey\n=\nvalue", "key", "value"),
                new Setting("SET\r\nkey=value;\r\n", "key", "value"),
                new Setting("-- leading comment\nSET key=value", "key", "value"),
                new Setting("/* leading */ SET key=value", "key", "value"),
                new Setting("SET/**/key/* key */=/* value */value", "key", "value"),
                new Setting("SET key=value -- trailing comment", "key", "value"),
                new Setting("SET key=value /* trailing comment */;", "key", "value"),
                new Setting("SET 'key'='value'", "key", "value"),
                new Setting("SET key='value'", "key", "value"),
                new Setting("SET 'key'=value", "key", "value"),
                new Setting("SET 'it''s.key'='it''s quoted'", "it's.key", "it's quoted"),
                new Setting("SET key='a;b=1 -- /* literal */'", "key", "a;b=1 -- /* literal */"),
                new Setting("SET key='http://host/path?a=b&c=d'", "key", "http://host/path?a=b&c=d"),
                new Setting("SET key='中文 值'", "key", "中文 值"),
                new Setting("SET sparse_lr=0.03", "sparse_lr", "0.03"),
                new Setting("SET offset=-1", "offset", "-1"),
                new Setting("SET rate=1e-3", "rate", "1e-3"),
                new Setting("SET enabled=true", "enabled", "true"),
                new Setting("SET enabled=FALSE", "enabled", "FALSE"),
                new Setting("SET key=null", "key", "null"),
                new Setting("SET hidden_units=64,32", "hidden_units", "64,32"),
                new Setting("SET normalizer=method=minmax,min=0,max=100", "normalizer", "method=minmax,min=0,max=100"),
                new Setting("SET key=''", "key", ""),
                new Setting("SET key=;", "key", ""),
                new Setting("SET key= -- empty\n", "key", "")
        ).map(example -> DynamicTest.dynamicTest(example.sql(), () -> {
            SqlSet set = assertInstanceOf(SqlSet.class, SqlRecSqlParser.parse(example.sql()));
            assertInstanceOf(SqlCharStringLiteral.class, set.getKey());
            assertInstanceOf(SqlCharStringLiteral.class, set.getValue());
            assertEquals(example.key(), set.getKeyString());
            assertEquals(example.value(), set.getValueString());
        }));
    }

    @TestFactory
    Stream<DynamicTest> defaultDatabaseCompatibilityUsesStandardNodes() {
        return Stream.of(
                "use default", "USE DEFAULT", "Use Default;", "use  default ",
                "USE\ndefault;", "/* before */ USE/**/DEFAULT -- after", "USE `default`",
                "show tables from default", "SHOW TABLES FROM DEFAULT;", "Show Tables In Default",
                "SHOW\nTABLES\tIN\nDEFAULT;", "/* before */ SHOW/**/TABLES IN/**/DEFAULT -- after",
                "SHOW TABLES IN `default`", "SHOW TABLES FROM default LIKE 'movie_%'",
                "SHOW TABLES IN default NOT LIKE 'tmp_%'"
        ).map(sql -> DynamicTest.dynamicTest(sql, () -> {
            SqlNode node = SqlRecSqlParser.parse(sql);
            if (node instanceof SqlUseDatabase use) {
                assertArrayEquals(new String[]{"default"}, use.fullDatabaseName());
            } else {
                SqlShowTables show = assertInstanceOf(SqlShowTables.class, node);
                assertArrayEquals(new String[]{"default"}, show.fullDatabaseName());
                if (sql.contains("NOT LIKE")) {
                    assertTrue(show.isNotLike());
                    assertEquals("tmp_%", show.getLikeSqlPattern());
                } else if (sql.contains("LIKE")) {
                    assertEquals("movie_%", show.getLikeSqlPattern());
                }
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> rejectsMalformedCommandsAndMultipleStatements() {
        return Stream.of(
                "usedefault", "use default extra", "USE DEFAULT.other", "USE DEFAULT DEFAULT",
                "showtables from default", "SHOW TABLES IN DEFAULT extra", "SHOW TABLES IN DEFAULT LIKE",
                "SET key", "SET =value", "SET key value", "SET key value=test",
                "SET key='unterminated", "SET key=value extra", "SET key='value' extra",
                "SET key=foo/* comment */bar", "SET key=foo\nbar", "SET key=(value)",
                "SET key='value'; SELECT 1", "SET key=value; SET other=value",
                "USE default; SELECT 1", "SHOW TABLES IN default; DROP TABLE t",
                "SELECT 1; SELECT 2", "SELECT 'unterminated", "/* unclosed", "SET key=value /* unclosed"
        ).map(sql -> DynamicTest.dynamicTest(sql,
                () -> assertThrows(SqlParseException.class, () -> SqlRecSqlParser.parse(sql))));
    }

    @Test
    void bareAndQuotedSetWorkInIfBranches() throws Exception {
        SqlIfCache conditional = assertInstanceOf(SqlIfCache.class, SqlRecSqlParser.parse(
                "IF (SELECT TRUE) THEN (SET table.sql-dialect=default) ELSE (SET 'key'='it''s')"));
        SqlSet thenSet = assertInstanceOf(SqlSet.class, conditional.getThenClause());
        assertEquals("table.sql-dialect", thenSet.getKeyString());
        assertEquals("default", thenSet.getValueString());
        SqlSet elseSet = assertInstanceOf(SqlSet.class, conditional.getElseClause());
        assertEquals("it's", elseSet.getValueString());
    }

    @Test
    void setListingRemainsWithoutAssignment() throws Exception {
        for (String sql : List.of("SET", "set;", "/* before */ SET -- after")) {
            SqlSet set = assertInstanceOf(SqlSet.class, SqlRecSqlParser.parse(sql));
            assertNull(set.getKey());
            assertNull(set.getValue());
        }
    }

    @Test
    void preservesOriginalSourcePositions() throws Exception {
        SqlSet set = assertInstanceOf(SqlSet.class,
                SqlRecSqlParser.parse("-- comment\n  SET key=\n    value;"));
        assertEquals(2, set.getParserPosition().getLineNum());
        assertEquals(3, set.getParserPosition().getColumnNum());
        assertEquals(2, set.getKey().getParserPosition().getLineNum());
        assertEquals(7, set.getKey().getParserPosition().getColumnNum());
        assertEquals(3, set.getValue().getParserPosition().getLineNum());
        assertEquals(5, set.getValue().getParserPosition().getColumnNum());

        SqlParseException error = assertThrows(SqlParseException.class,
                () -> SqlRecSqlParser.parse("-- comment\nSET key='value' extra"));
        assertEquals(2, error.getPos().getLineNum());
        assertEquals(17, error.getPos().getColumnNum());
    }

    @Test
    void leavesStandardSqlAndParserConfigurationToFlink() throws Exception {
        SqlSelect select = assertInstanceOf(SqlSelect.class,
                SqlRecSqlParser.parse("SELECT 'use default; SET key=value' AS text FROM `default`.t"));
        assertEquals("use default; SET key=value",
                ((SqlCharStringLiteral) ((SqlCall) select.getSelectList().get(0))
                        .getOperandList().get(0)).getNlsString().getValue());
        assertArrayEquals(new String[]{"review"}, ((SqlUseDatabase) SqlRecSqlParser.parse("USE review")).fullDatabaseName());
        assertArrayEquals(new String[]{"review"}, ((SqlShowTables) SqlRecSqlParser.parse("SHOW TABLES IN review")).fullDatabaseName());
        assertInstanceOf(SqlSelect.class, SqlRecSqlParser.parse("SELECT \"column\" FROM \"table\"",
                SqlRecSqlParser.defaultConfig().withLex(Lex.ORACLE)));
        assertThrows(SqlParseException.class, () -> SqlRecSqlParser.parse("SELECT FROM"));
        assertThrows(SqlParseException.class, () -> SqlRecSqlParser.parse("SELECT default FROM t"));
    }

    @Test
    void compatibilityKeywordsRemainUsableAsSqlRecIdentifiers() throws Exception {
        for (String name : List.of("use", "tables")) {
            SqlCache cache = assertInstanceOf(SqlCache.class,
                    SqlRecSqlParser.parse("CACHE TABLE " + name + " AS SELECT 1"));
            assertEquals(name, cache.getTableName().getSimple());
            SqlCreateModel model = assertInstanceOf(SqlCreateModel.class,
                    SqlRecSqlParser.parse("CREATE MODEL " + name + " (" + name + " FLOAT)"));
            assertEquals(name, model.getModelName().getSimple());
            assertEquals(name, ((SqlRecColumnDeclaration) model.getFieldList().get(0)).getName().getSimple());
        }
    }
}
