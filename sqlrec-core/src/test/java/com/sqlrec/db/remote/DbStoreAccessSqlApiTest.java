package com.sqlrec.db.remote;

import com.sqlrec.entity.SqlApi;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that DbStoreAccess normalizes sql api function names on write:
 * queries (see getSqlApiListByFunctionName) filter with an upper-cased
 * parameter while postgres string comparison is case-sensitive, so a
 * mixed-case written value would never be found.
 */
class DbStoreAccessSqlApiTest {

    @Test
    void insertSqlApiNormalizesFunctionNameToUpperCase() {
        DbMapper mapper = mock(DbMapper.class);
        DbStoreAccess access = testableAccess(mapper);

        SqlApi sqlApi = new SqlApi();
        sqlApi.setName("api1");
        sqlApi.setFunctionName("myFunc");
        access.insertSqlApi(sqlApi);

        verify(mapper).insertSqlApi(argThat(a -> "MYFUNC".equals(a.getFunctionName())));
        assertEquals("MYFUNC", sqlApi.getFunctionName());
    }

    @Test
    void upsertSqlApiNormalizesFunctionNameToUpperCase() {
        DbMapper mapper = mock(DbMapper.class);
        DbStoreAccess access = testableAccess(mapper);

        SqlApi sqlApi = new SqlApi();
        sqlApi.setName("api1");
        sqlApi.setFunctionName("myFunc");
        access.upsertSqlApi(sqlApi);

        verify(mapper).upsertSqlApi(argThat(a -> "MYFUNC".equals(a.getFunctionName())));
    }

    @Test
    void getSqlApiListByFunctionNameQueriesWithUpperCase() {
        DbMapper mapper = mock(DbMapper.class);
        DbStoreAccess access = testableAccess(mapper);

        access.getSqlApiListByFunctionName("myFunc");

        verify(mapper).getSqlApiListByFunctionName("MYFUNC");
    }

    private static DbStoreAccess testableAccess(DbMapper mapper) {
        SqlSession session = mock(SqlSession.class);
        when(session.getMapper(DbMapper.class)).thenReturn(mapper);
        SqlSessionFactory factory = mock(SqlSessionFactory.class);
        when(factory.openSession()).thenReturn(session);
        return new DbStoreAccess() {
            @Override
            public SqlSessionFactory getSqlSessionFactory() {
                return factory;
            }
        };
    }
}
