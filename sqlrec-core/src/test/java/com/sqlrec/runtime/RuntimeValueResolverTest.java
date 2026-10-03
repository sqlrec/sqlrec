package com.sqlrec.runtime;

import com.sqlrec.sql.parser.SqlGetVariable;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import static com.sqlrec.runtime.RuntimeValueResolver.MissingValuePolicy.NULL_ONLY;
import static com.sqlrec.runtime.RuntimeValueResolver.MissingValuePolicy.NULL_OR_EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RuntimeValueResolverTest {
    @Test
    void preservesDifferentDefaultRulesForFunctionNamesAndStringArguments() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        SqlGetVariable variable = new SqlGetVariable(SqlParserPos.ZERO,
                SqlLiteral.createCharString("target", SqlParserPos.ZERO),
                SqlLiteral.createCharString("fallback", SqlParserPos.ZERO));

        assertEquals("fallback", RuntimeValueResolver.resolveVariable(variable, context, NULL_ONLY));
        assertEquals("fallback", RuntimeValueResolver.resolveVariable(variable, context, NULL_OR_EMPTY));
        context.setVariable("target", "");
        assertEquals("", RuntimeValueResolver.resolveVariable(variable, context, NULL_ONLY));
        assertEquals("fallback", RuntimeValueResolver.resolveVariable(variable, context, NULL_OR_EMPTY));
        for (String value : new String[]{" ", "override"}) {
            context.setVariable("target", value);
            assertEquals(value, RuntimeValueResolver.resolveVariable(variable, context, NULL_ONLY));
            assertEquals(value, RuntimeValueResolver.resolveVariable(variable, context, NULL_OR_EMPTY));
        }
    }

    @Test
    void leavesMissingValuesAndEmptyDefaultsForTheCallerToValidate() {
        ExecuteContextImpl context = new ExecuteContextImpl();
        SqlGetVariable withoutDefault = new SqlGetVariable(SqlParserPos.ZERO,
                SqlLiteral.createCharString("size", SqlParserPos.ZERO));
        assertNull(RuntimeValueResolver.resolveVariable(withoutDefault, context, NULL_ONLY));
        assertNull(RuntimeValueResolver.resolveVariable(withoutDefault, context, NULL_OR_EMPTY));
        context.setVariable("size", "");
        assertEquals("", RuntimeValueResolver.resolveVariable(withoutDefault, context, NULL_OR_EMPTY));

        SqlGetVariable emptyDefault = new SqlGetVariable(SqlParserPos.ZERO,
                SqlLiteral.createCharString("unset", SqlParserPos.ZERO),
                SqlLiteral.createCharString("", SqlParserPos.ZERO));
        assertEquals("", RuntimeValueResolver.resolveVariable(emptyDefault, context, NULL_ONLY));
        assertEquals("", RuntimeValueResolver.resolveVariable(emptyDefault, context, NULL_OR_EMPTY));
    }
}
