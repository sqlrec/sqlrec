package com.sqlrec.db;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class MetadataAccessTest {
    @Test
    public void testInsertSqlFunction() {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlFunction sqlFunction = new SqlFunction();
        String name = "test_function_" + UUID.randomUUID().toString().replace("-", "");
        sqlFunction.setName(name);
        sqlFunction.setSqlList("[\"select 1\"]");
        sqlFunction.setCreatedAt(1000L);
        sqlFunction.setUpdatedAt(1000L);
        try {
            assertNull(db.getSqlFunction(name));
            db.insertSqlFunction(sqlFunction);
            assertFunctionEquals(sqlFunction, db.getSqlFunction(name));

            SqlFunction updated = new SqlFunction();
            updated.setName(name);
            updated.setSqlList("[\"select 2\"]");
            updated.setCreatedAt(sqlFunction.getCreatedAt());
            updated.setUpdatedAt(2000L);
            db.upsertSqlFunction(updated);
            assertFunctionEquals(updated, db.getSqlFunction(name));

            db.deleteSqlFunction(name);
            assertNull(db.getSqlFunction(name));
        } finally {
            db.deleteSqlFunction(name);
        }
    }

    @Test
    public void testInsertSqlApi() {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlApi sqlApi = new SqlApi();
        String name = "test_api_" + UUID.randomUUID().toString().replace("-", "");
        sqlApi.setName(name);
        sqlApi.setFunctionName("first_function");
        sqlApi.setCreatedAt(1000L);
        sqlApi.setUpdatedAt(1000L);
        try {
            assertNull(db.getSqlApi(name));
            db.insertSqlApi(sqlApi);
            assertApiEquals(sqlApi, db.getSqlApi(name));

            SqlApi updated = new SqlApi();
            updated.setName(name);
            updated.setFunctionName("second_function");
            updated.setCreatedAt(sqlApi.getCreatedAt());
            updated.setUpdatedAt(2000L);
            db.upsertSqlApi(updated);
            assertApiEquals(updated, db.getSqlApi(name));

            db.deleteSqlApi(name);
            assertNull(db.getSqlApi(name));
        } finally {
            db.deleteSqlApi(name);
        }
    }

    private void assertFunctionEquals(SqlFunction expected, SqlFunction actual) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getSqlList(), actual.getSqlList());
        assertEquals(expected.getCreatedAt(), actual.getCreatedAt());
        assertEquals(expected.getUpdatedAt(), actual.getUpdatedAt());
    }

    private void assertApiEquals(SqlApi expected, SqlApi actual) {
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getFunctionName(), actual.getFunctionName());
        assertEquals(expected.getCreatedAt(), actual.getCreatedAt());
        assertEquals(expected.getUpdatedAt(), actual.getUpdatedAt());
    }
}
