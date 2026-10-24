package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.udf.scalar.GetFunction;
import com.sqlrec.udf.scalar.GetOrDefaultFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExecuteContextImplVariableTest {
    private final boolean originalFallback = SqlRecConfigs.ENABLE_VARIABLE_ENV_FALLBACK.getDefaultValue();
    private final CalciteSchema schema = CalciteSchema.createRootSchema(false);
    private final ExecuteContextImpl context = new ExecuteContextImpl(schema);

    @AfterEach
    void restoreConfig() {
        SqlRecConfigs.ENABLE_VARIABLE_ENV_FALLBACK.setDefaultValue(originalFallback);
    }

    @Test
    void fallsBackWithoutAddingEnvironmentToExecutionVariables() {
        assertNotNull(System.getenv("PATH"));
        assertEquals(System.getenv("PATH"), context.getVariable("PATH"));
        assertEquals(System.getenv("PATH"), context.clone().getVariable("PATH"));
        assertFalse(context.getVariables().containsKey("PATH"));
        var dataContext = new SqlRecDataContextImpl(Map.of(), schema, context);
        assertEquals(System.getenv("PATH"), GetFunction.evaluate(dataContext, "PATH"));
        assertEquals(System.getenv("PATH"), GetOrDefaultFunction.evaluate(dataContext, "PATH", "default"));
    }

    @Test
    void explicitValuesIncludingEmptyStringsOverrideEnvironmentAndRemovalRestoresFallback() {
        context.setVariable("PATH", "override");
        assertEquals("override", context.getVariable("PATH"));
        context.setVariable("PATH", "");
        assertEquals("", context.getVariable("PATH"));
        context.setVariable("PATH", null);
        assertEquals(System.getenv("PATH"), context.getVariable("PATH"));
    }

    @Test
    void disabledFallbackCannotBeEnabledByExecutionVariables() {
        SqlRecConfigs.ENABLE_VARIABLE_ENV_FALLBACK.setDefaultValue(false);
        context.setVariable("ENABLE_VARIABLE_ENV_FALLBACK", "true");
        assertNull(context.getVariable("PATH"));
        var dataContext = new SqlRecDataContextImpl(Map.of(), schema, context);
        assertEquals("default", GetOrDefaultFunction.evaluate(dataContext, "PATH", "default"));
        context.setVariable("PATH", "explicit");
        assertEquals("explicit", context.getVariable("PATH"));
    }

    @Test
    void missingInBothSourcesReturnsNullAndAllowsCallerDefault() {
        String key = "SQLREC_TEST_MISSING_VARIABLE_8A4E63A1";
        assertNull(System.getenv(key));
        assertNull(context.getVariable(key));
        var dataContext = new SqlRecDataContextImpl(Map.of(), schema, context);
        assertEquals("default", GetOrDefaultFunction.evaluate(dataContext, key, "default"));
    }
}
