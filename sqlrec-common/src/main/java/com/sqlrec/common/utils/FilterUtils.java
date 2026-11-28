package com.sqlrec.common.utils;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;

/** Backend-neutral helpers for filter predicates and patterns. */
public final class FilterUtils {
    private FilterUtils() {
    }

    /** Converts a LIKE pattern to an anchored regex. A dangling escape is literal only when allowed. */
    public static String likeToRegex(String pattern, Character escape, boolean literalTrailingEscape) {
        if (pattern == null) {
            return null;
        }
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escape != null && c == escape) {
                if (i + 1 < pattern.length()) {
                    c = pattern.charAt(++i);
                } else if (!literalTrailingEscape) {
                    return null;
                }
            } else if (c == '%') {
                regex.append(".*");
                continue;
            } else if (c == '_') {
                regex.append('.');
                continue;
            }
            if ("\\.^$|?*+()[]{}".indexOf(c) >= 0) {
                regex.append('\\');
            }
            regex.append(c);
        }
        return regex.append('$').toString();
    }

    public static boolean isPointRange(Range<?> range) {
        return range.hasLowerBound() && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint().equals(range.upperEndpoint());
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
