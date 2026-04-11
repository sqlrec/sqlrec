package com.sqlrec.compiler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlPreProcesserTest {

    @Test
    public void testPreProcessSqlWithNull() {
        String result = SqlPreProcesser.preProcessSql(null);
        assertNull(result);
    }

    @Test
    public void testPreProcessSqlWithEmpty() {
        String result = SqlPreProcesser.preProcessSql("");
        assertEquals("", result);
    }

    @Test
    public void testPreProcessSqlWithUseDefault() {
        String result = SqlPreProcesser.preProcessSql("use default");
        assertEquals("use `default`", result);
    }

    @Test
    public void testPreProcessSqlWithUseDefaultWithSpaces() {
        String result = SqlPreProcesser.preProcessSql("use  default ");
        assertEquals("use `default`", result);
    }

    @Test
    public void testPreProcessSqlWithUseDefaultUpperCase() {
        String result = SqlPreProcesser.preProcessSql("USE DEFAULT");
        assertEquals("USE DEFAULT", result);
    }

    @Test
    public void testPreProcessSqlWithShowTablesFromDefault() {
        String result = SqlPreProcesser.preProcessSql("show tables from default");
        assertEquals("show tables from `default`", result);
    }

    @Test
    public void testPreProcessSqlWithShowTablesInDefault() {
        String result = SqlPreProcesser.preProcessSql("show tables in default");
        assertEquals("show tables in `default`", result);
    }

    @Test
    public void testIsSetStatementWithSetKeyword() {
        assertTrue(SqlPreProcesser.isSetStatement("set param=value"));
        assertTrue(SqlPreProcesser.isSetStatement("SET param=value"));
        assertTrue(SqlPreProcesser.isSetStatement(" set param=value"));
    }

    @Test
    public void testIsSetStatementWithNonSetKeyword() {
        assertFalse(SqlPreProcesser.isSetStatement("select * from t"));
        assertFalse(SqlPreProcesser.isSetStatement("insert into t values(1)"));
        assertFalse(SqlPreProcesser.isSetStatement("reset"));
    }

    @Test
    public void testTransformSetStatementBasic() {
        String result = SqlPreProcesser.transformSetStatement("set param=test");
        assertEquals("set 'param'='test'", result);
    }

    @Test
    public void testTransformSetStatementWithSpaces() {
        String result = SqlPreProcesser.transformSetStatement("set param = test");
        assertEquals("set 'param'='test'", result);
    }

    @Test
    public void testTransformSetStatementWithTrim() {
        String result = SqlPreProcesser.transformSetStatement("  set param=test  ");
        assertEquals("set 'param'='test'", result);
    }

    @Test
    public void testTransformSetStatementWithAlreadyQuoted() {
        String result = SqlPreProcesser.transformSetStatement("set 'param'='test'");
        assertEquals("set 'param'='test'", result);
    }

    @Test
    public void testTransformSetStatementWithMultipleEquals() {
        String result = SqlPreProcesser.transformSetStatement("set param=test=value");
        assertEquals("set param=test=value", result);
    }

    @Test
    public void testTransformSetStatementWithNoEquals() {
        String result = SqlPreProcesser.transformSetStatement("set param");
        assertEquals("set param", result);
    }

    @Test
    public void testPreProcessSqlWithSetStatement() {
        String result = SqlPreProcesser.preProcessSql("set param=test");
        assertEquals("set 'param'='test'", result);
    }

    @Test
    public void testPreProcessSqlWithNormalSql() {
        String sql = "select * from my_table";
        String result = SqlPreProcesser.preProcessSql(sql);
        assertEquals(sql, result);
    }

    @Test
    public void testPreProcessSqlWithMixedCaseUseDefault() {
        String result = SqlPreProcesser.preProcessSql("Use Default");
        assertEquals("Use Default", result);
    }
}
