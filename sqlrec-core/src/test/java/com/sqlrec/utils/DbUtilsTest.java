package com.sqlrec.utils;

import com.sqlrec.entity.SqlFunction;

public class DbUtilsTest {
    public static void main(String[] args) {
//        SqlFunction sqlFunction = new SqlFunction();
//        sqlFunction.setName("test");
//        sqlFunction.setSqlList("select 1");
//        DbUtils.insertSqlFunction(sqlFunction);
        SqlFunction sqlFunction1 = DbUtils.getSqlFunction("test");
        System.out.println(sqlFunction1);
    }
}