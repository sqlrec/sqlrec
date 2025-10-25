package com.sqlrec.utils;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class DbUtilsTest {
    @Test
    public void testInsertSqlFunction() {
        SqlFunction sqlFunction = new SqlFunction();
        DbUtils.deleteSqlFunction("test");
        sqlFunction.setName("test");
        sqlFunction.setSqlList("[\"select 1\"]");
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        DbUtils.insertSqlFunction(sqlFunction);
        DbUtils.upsertSqlFunction(sqlFunction);
        SqlFunction sqlFunction1 = DbUtils.getSqlFunction("test");
        System.out.println(sqlFunction1);
        DbUtils.deleteSqlFunction("test");
    }

    @Test
    public void testInsertSqlApi() {
        SqlApi sqlApi = new SqlApi();
        DbUtils.deleteSqlApi("test");
        sqlApi.setName("test");
        sqlApi.setFunctionName("test");
        sqlApi.setCreatedAt(System.currentTimeMillis());
        sqlApi.setUpdatedAt(System.currentTimeMillis());
        DbUtils.insertSqlApi(sqlApi);
        DbUtils.upsertSqlApi(sqlApi);
        SqlApi sqlApi1 = DbUtils.getSqlApi("test");
        System.out.println(sqlApi1);
        DbUtils.deleteSqlApi("test");
    }
}