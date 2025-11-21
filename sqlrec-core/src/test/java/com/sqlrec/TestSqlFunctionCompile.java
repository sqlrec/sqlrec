package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestSqlFunctionCompile {
    @Test
    public void testSqlCompile() throws Exception {
        List<String> fun1SqlList = Arrays.asList(
                "create sql function fun1",
                "return"
        );
        CompileManager.compileSqlFunction("fun1", fun1SqlList);

        List<String> fun2SqlList = Arrays.asList(
                "create sql function fun2",
                "call fun1()",
                "return"
        );
        CompileManager.compileSqlFunction("fun2", fun2SqlList);

        fun1SqlList = Arrays.asList(
                "create sql function fun1",
                "call fun2()",
                "return"
        );
        Exception e = null;
        try {
            CompileManager.compileSqlFunction("fun1", fun1SqlList);
        } catch (Exception ex) {
            e = ex;
        }
        assert e != null;
        assert e.getMessage().contains("circular dependency: FUN2->FUN1");
    }
}
