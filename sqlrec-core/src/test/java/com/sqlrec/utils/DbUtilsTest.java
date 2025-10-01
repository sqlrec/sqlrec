package com.sqlrec.utils;

import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

@Tag("integration")
public class DbUtilsTest {
    @Test
    public void testInsertSqlFunction() {
        SqlFunction sqlFunction = new SqlFunction();
        DbUtils.deleteSqlFunction("test");
        sqlFunction.setName("test");
        sqlFunction.setSqlList("[\"select 1\"]");
        DbUtils.insertSqlFunction(sqlFunction);
        SqlFunction sqlFunction1 = DbUtils.getSqlFunction("test");
        System.out.println(sqlFunction1);
    }
}