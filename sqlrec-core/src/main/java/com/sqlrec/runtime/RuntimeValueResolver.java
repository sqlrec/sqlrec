package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlCharStringLiteral;

/** Resolves runtime variables without imposing a caller's validation or error messages. */
final class RuntimeValueResolver {
    enum MissingValuePolicy {
        NULL_ONLY,
        NULL_OR_EMPTY
    }

    private RuntimeValueResolver() {
    }

    static String variableName(SqlGetVariable variable) {
        return SchemaUtils.getValueOfStringLiteral(variable.getVariableName());
    }

    static String resolveVariable(
            SqlGetVariable variable, ReadonlyContext context, MissingValuePolicy policy) {
        String value = context.getVariable(variableName(variable));
        boolean missing = value == null || (policy == MissingValuePolicy.NULL_OR_EMPTY && value.isEmpty());
        if (missing && variable.hasDefaultValue()) {
            return SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) variable.getDefaultValue());
        }
        return value;
    }
}
