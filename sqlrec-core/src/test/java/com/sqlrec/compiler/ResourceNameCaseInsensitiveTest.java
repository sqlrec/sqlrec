package com.sqlrec.compiler;

import com.sqlrec.runtime.SqlFunctionBindable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Resource names are case-insensitive: a function defined with any casing can be
 * compiled/looked up with any casing, and the normalized (lower case) name is
 * what flows through the system.
 */
class ResourceNameCaseInsensitiveTest {

    private static final String UPPER_NAME = "Mixed_Case_Fun";

    @Test
    void compileSqlFunctionIsCaseInsensitiveAndStoresLowerCaseName() throws Exception {
        SqlFunctionBindable bindable = new CompileManager().compileSqlFunction(
                UPPER_NAME,
                Arrays.asList(
                        "create sql function " + UPPER_NAME,
                        "cache table r as select 1 as a",
                        "return r")
        );

        assertEquals("mixed_case_fun", bindable.getFunName());
    }

    @Test
    void getSqlFunctionFindsCompiledFunctionWithAnyCasing() throws Exception {
        CompileManager compileManager = new CompileManager();
        compileManager.compileSqlFunction(UPPER_NAME, Arrays.asList(
                "create sql function " + UPPER_NAME,
                "cache table r as select 1 as a",
                "return r"));

        SqlFunctionBindable lower = compileManager.getSqlFunction("mixed_case_fun");
        SqlFunctionBindable mixed = compileManager.getSqlFunction("MiXeD_cAsE_fUn");

        assertSame(lower, mixed);
        assertEquals("mixed_case_fun", mixed.getFunName());
    }
}
