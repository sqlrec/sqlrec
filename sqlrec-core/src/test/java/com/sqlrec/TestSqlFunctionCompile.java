package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.utils.DbUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@Tag("integration")
public class TestSqlFunctionCompile {
    @Test
    public void testSqlCompile() throws Exception {
        List<String> fun1SqlList = Arrays.asList(
                "create sql function fun1",
                "return"
        );
        new CompileManager().compileSqlFunction("fun1", fun1SqlList);

        List<String> fun2SqlList = Arrays.asList(
                "create sql function fun2",
                "call fun1()",
                "return"
        );
        new CompileManager().compileSqlFunction("fun2", fun2SqlList);

        fun1SqlList = Arrays.asList(
                "create sql function fun1",
                "call fun2()",
                "return"
        );
        Exception e = null;
        try {
            new CompileManager().compileSqlFunction("fun1", fun1SqlList);
        } catch (Exception ex) {
            e = ex;
        }
        assert e != null;
        assert e.getMessage().contains("circular dependency: FUN2->FUN1");
    }

    @Test
    public void testSqlCompileInDb() throws Exception {
        SqlFunction sqlFunction = new SqlFunction();
        DbUtils.deleteSqlFunction("test_compile1");
        sqlFunction.setName("test_compile1");
        sqlFunction.setSqlList("[\"create sql function test_compile1\", \"call test_compile2()\", \"return\"]");
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        DbUtils.insertSqlFunction(sqlFunction);

        SqlFunction sqlFunction2 = new SqlFunction();
        DbUtils.deleteSqlFunction("test_compile2");
        sqlFunction2.setName("test_compile2");
        sqlFunction2.setSqlList("[\"create sql function test_compile2\", \"call test_compile1()\", \"return\"]");
        sqlFunction2.setCreatedAt(System.currentTimeMillis());
        sqlFunction2.setUpdatedAt(System.currentTimeMillis());
        DbUtils.insertSqlFunction(sqlFunction2);

        Exception e = null;
        try {
            new CompileManager().getSqlFunction("test_compile1");
        } catch (Exception ex) {
            e = ex;
        }
        assert e != null;
        assert e.getMessage().contains("circular dependency: TEST_COMPILE1 trace: TEST_COMPILE1->TEST_COMPILE2");

        DbUtils.deleteSqlFunction("test_compile1");
        DbUtils.deleteSqlFunction("test_compile2");
    }
}
