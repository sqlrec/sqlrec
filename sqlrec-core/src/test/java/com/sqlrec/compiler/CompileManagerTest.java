package com.sqlrec.compiler;

import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.utils.DbUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class CompileManagerTest {

    @Test
    void updateFunctionBindable() throws Exception {
        SqlFunction sqlFunction = new SqlFunction();
        sqlFunction.setName("test");
        sqlFunction.setSqlList("[\"create sql function test\", \"cache table t as select 1 as a\", \"return t\"]");
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        DbUtils.upsertSqlFunction(sqlFunction);

        SqlFunctionBindable sqlFunctionBindable1 = new CompileManager().getSqlFunction("test");
        Thread.sleep(1);
        FunctionUpdater.updateFunctionBindable();
        SqlFunctionBindable sqlFunctionBindable2 = new CompileManager().getSqlFunction("test");
        assertEquals(sqlFunctionBindable1, sqlFunctionBindable2);

        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        DbUtils.upsertSqlFunction(sqlFunction);
        SqlFunction sqlFunction2 = DbUtils.getSqlFunction("test");
        assertEquals(sqlFunction2.getUpdatedAt(), sqlFunction.getUpdatedAt());

        FunctionUpdater.updateFunctionBindable();
        SqlFunctionBindable sqlFunctionBindable3 = new CompileManager().getSqlFunction("test");
        assertNotEquals(sqlFunctionBindable1, sqlFunctionBindable3);
        assertTrue(sqlFunctionBindable3.getCreateTime() > sqlFunctionBindable1.getCreateTime());

        DbUtils.deleteSqlFunction("test");
        FunctionUpdater.updateFunctionBindable();
        assertThrows(Exception.class, () -> new CompileManager().getSqlFunction("test"));
    }
}