package com.sqlrec.db;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class MetadataAccessTest {
    @Test
    public void testInsertSqlFunction() {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlFunction sqlFunction = new SqlFunction();
        db.deleteSqlFunction("test");
        sqlFunction.setName("test");
        sqlFunction.setSqlList("[\"select 1\"]");
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        db.insertSqlFunction(sqlFunction);
        db.upsertSqlFunction(sqlFunction);
        SqlFunction sqlFunction1 = db.getSqlFunction("test");
        System.out.println(sqlFunction1);
        db.deleteSqlFunction("test");
    }

    @Test
    public void testInsertSqlApi() {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlApi sqlApi = new SqlApi();
        db.deleteSqlApi("test");
        sqlApi.setName("test");
        sqlApi.setFunctionName("test");
        sqlApi.setCreatedAt(System.currentTimeMillis());
        sqlApi.setUpdatedAt(System.currentTimeMillis());
        db.insertSqlApi(sqlApi);
        db.upsertSqlApi(sqlApi);
        SqlApi sqlApi1 = db.getSqlApi("test");
        System.out.println(sqlApi1);
        db.deleteSqlApi("test");
    }
}
