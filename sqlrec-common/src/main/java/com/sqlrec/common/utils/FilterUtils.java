package com.sqlrec.common.utils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;

/** Backend-neutral helpers for inspecting Calcite filter predicates. */
public final class FilterUtils {
    private FilterUtils() {
    }

    public static Object extractPrimaryKeyValue(List<RexNode> filters, int primaryKeyIndex) {
        if (filters == null || filters.size() != 1 || primaryKeyIndex < 0) {
            return null;
        }
        RexNode filter = filters.get(0);
        if (!filter.isA(SqlKind.EQUALS)) {
            return null;
        }
        RexCall call = (RexCall) filter;
        Object value = extractPrimaryKeyValue(
                call.getOperands().get(0), call.getOperands().get(1), primaryKeyIndex);
        return value != null ? value : extractPrimaryKeyValue(
                call.getOperands().get(1), call.getOperands().get(0), primaryKeyIndex);
    }

    private static Object extractPrimaryKeyValue(
            RexNode field, RexNode value, int primaryKeyIndex) {
        if (!(field instanceof RexInputRef) || !(value instanceof RexLiteral)) {
            return null;
        }
        return ((RexInputRef) field).getIndex() == primaryKeyIndex
                ? ((RexLiteral) value).getValue2() : null;
    }

    public static List<RexNode> getPrimaryKeyFilters(List<RexNode> filters, int primaryKeyIndex) {
        if (filters == null || primaryKeyIndex < 0) {
            return Collections.emptyList();
        }
        for (RexNode filter : filters) {
            if (filter.isA(SqlKind.EQUALS) && referencesField((RexCall) filter, primaryKeyIndex)) {
                return Collections.singletonList(filter);
            }
            if (filter.isA(SqlKind.AND)) {
                List<RexNode> nested = getPrimaryKeyFilters(
                        ((RexCall) filter).getOperands(), primaryKeyIndex);
                if (!nested.isEmpty()) {
                    return nested;
                }
            }
        }
        return Collections.emptyList();
    }

    private static boolean referencesField(RexCall call, int fieldIndex) {
        return call.getOperands().stream().anyMatch(operand -> operand instanceof RexInputRef
                && ((RexInputRef) operand).getIndex() == fieldIndex);
    }
}
