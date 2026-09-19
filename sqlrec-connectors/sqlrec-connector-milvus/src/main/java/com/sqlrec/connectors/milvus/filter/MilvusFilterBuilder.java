package com.sqlrec.connectors.milvus.filter;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Translates Calcite predicates to Milvus filter expressions. */
public final class MilvusFilterBuilder {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Set<String> ARRAY_OPERATORS = new HashSet<>(Arrays.asList(
            "array_contains", "array_contains_all", "array_contains_any"));

    private MilvusFilterBuilder() {
    }

    /** Builds a non-empty predicate that matches every row in a Milvus collection. */
    public static String buildMatchAllFilter(List<FieldSchema> fields, Integer primaryKeyIndex) {
        if (fields == null || primaryKeyIndex == null
                || primaryKeyIndex < 0 || primaryKeyIndex >= fields.size()) {
            throw new IllegalArgumentException("Invalid Milvus primary key index: " + primaryKeyIndex);
        }
        // Milvus primary-key fields are never nullable.
        return identifier(fields.get(primaryKeyIndex).getName()) + " IS NOT NULL";
    }

    /** Builds a filter for a regular Milvus query. */
    public static String buildScanFilter(List<RexNode> filters, List<FieldSchema> fields) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        List<String> expressions = new ArrayList<>(filters.size());
        for (RexNode filter : filters) {
            String expression = buildScanFilter(filter, fields);
            if (expression == null) {
                return null;
            }
            expressions.add(expression);
        }
        return String.join(" AND ", expressions);
    }

    private static String buildScanFilter(RexNode node, List<FieldSchema> fields) {
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR)) {
            String delimiter = call.isA(SqlKind.AND) ? " AND " : " OR ";
            List<String> expressions = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                String expression = buildScanFilter(operand, fields);
                if (expression == null) {
                    return null;
                }
                expressions.add("(" + expression + ")");
            }
            return String.join(delimiter, expressions);
        }
        String operator = call.getOperator().getName();
        if (isArrayOperator(operator)) {
            return buildScanArray(call, fields);
        }
        if (call.isA(SqlKind.SEARCH)) {
            return buildSearch(call, fields);
        }
        if (call.isA(SqlKind.LIKE)) {
            return buildLike(call, fields);
        }
        if (call.isA(SqlKind.IS_NULL) || call.isA(SqlKind.IS_NOT_NULL)) {
            if (call.getOperands().size() != 1
                    || !(call.getOperands().get(0) instanceof RexInputRef)) {
                return null;
            }
            String operand = scanOperand(call.getOperands().get(0), fields);
            return operand == null ? null : operand
                    + (call.isA(SqlKind.IS_NULL) ? " IS NULL" : " IS NOT NULL");
        }
        if (call.getOperands().size() != 2 || !isComparison(call.getKind())) {
            return null;
        }
        String first = scanOperand(call.getOperands().get(0), fields);
        String second = scanOperand(call.getOperands().get(1), fields);
        return first == null || second == null ? null
                : first + " " + operator(operator) + " " + second;
    }

    private static String buildScanArray(RexCall call, List<FieldSchema> fields) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)) {
            return null;
        }
        RexNode valueNode = call.getOperands().get(1);
        if (requiresArrayArgument(call.getOperator().getName())
                && !valueNode.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)
                && (!(valueNode instanceof RexLiteral)
                        || !(((RexLiteral) valueNode).getValue() instanceof List))) {
            return null;
        }
        String field = scanOperand(call.getOperands().get(0), fields);
        String value = scanConstant(valueNode, fields);
        return field == null || value == null ? null
                : call.getOperator().getName().toLowerCase(Locale.ROOT)
                        + "(" + field + ", " + value + ")";
    }

    private static String scanConstant(RexNode node, List<FieldSchema> fields) {
        if (node instanceof RexLiteral) {
            return scanOperand(node, fields);
        }
        if (!node.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            return null;
        }
        List<String> values = new ArrayList<>();
        for (RexNode operand : ((RexCall) node).getOperands()) {
            if (!(operand instanceof RexLiteral)) {
                return null;
            }
            values.add(scanOperand(operand, fields));
        }
        return "[" + String.join(", ", values) + "]";
    }

    private static String buildLike(RexCall call, List<FieldSchema> fields) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !(call.getOperands().get(1) instanceof RexLiteral)) {
            return null;
        }
        Object pattern = ((RexLiteral) call.getOperands().get(1)).getValue();
        if (!(pattern instanceof NlsString) && !(pattern instanceof String)) {
            return null;
        }
        String field = scanOperand(call.getOperands().get(0), fields);
        String value = scanOperand(call.getOperands().get(1), fields);
        return field == null || value == null ? null : field + " like " + value;
    }

    private static String buildSearch(RexCall call, List<FieldSchema> fields) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !(call.getOperands().get(1) instanceof RexLiteral)) {
            return null;
        }
        Sarg<?> sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
        if (sarg == null || sarg.nullAs == RexUnknownAs.TRUE
                || (!sarg.isPoints() && !sarg.isComplementedPoints())) {
            return null;
        }
        List<String> values = new ArrayList<>();
        Iterable<? extends Range<?>> ranges = sarg.isComplementedPoints()
                ? sarg.rangeSet.complement().asRanges()
                : sarg.rangeSet.asRanges();
        for (Range<?> range : ranges) {
            if (!range.hasLowerBound() || !range.hasUpperBound()
                    || range.lowerBoundType() != BoundType.CLOSED
                    || range.upperBoundType() != BoundType.CLOSED
                    || !range.lowerEndpoint().equals(range.upperEndpoint())) {
                return null;
            }
            values.add(formatValue(range.lowerEndpoint()));
        }
        if (values.isEmpty()) {
            return "false";
        }
        String field = scanOperand(call.getOperands().get(0), fields);
        return field == null ? null : field
                + (sarg.isComplementedPoints() ? " NOT IN [" : " IN [")
                        + String.join(", ", values) + "]";
    }

    private static String scanOperand(RexNode node, List<FieldSchema> fields) {
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return index < 0 || index >= fields.size()
                    ? null : identifier(fields.get(index).getName());
        }
        if (node instanceof RexLiteral) {
            return formatValue(((RexLiteral) node).getValue());
        }
        if (node.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            List<String> values = new ArrayList<>();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                String value = scanOperand(operand, fields);
                if (value == null) {
                    return null;
                }
                values.add(value);
            }
            return "[" + String.join(", ", values) + "]";
        }
        return null;
    }

    /** Builds a filter whose left-side references are resolved from the current join row. */
    public static String buildJoinFilter(
            RexNode condition, Object[] leftRow, List<String> rightFieldNames) {
        if (condition == null) {
            return null;
        }
        rightFieldNames.forEach(MilvusFilterBuilder::identifier);
        return buildJoinFilter(condition, leftRow, leftRow == null ? 0 : leftRow.length,
                rightFieldNames);
    }

    private static String buildJoinFilter(
            RexNode node, Object[] leftRow, int leftSize, List<String> rightFields) {
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR)) {
            String delimiter = call.isA(SqlKind.AND) ? " and " : " or ";
            List<String> expressions = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                String expression = buildJoinFilter(operand, leftRow, leftSize, rightFields);
                if (expression == null) {
                    return null;
                }
                expressions.add(expression);
            }
            return "(" + String.join(delimiter, expressions) + ")";
        }
        if (isArrayOperator(call.getOperator().getName())) {
            return buildJoinArray(call, leftRow, leftSize, rightFields);
        }
        if (!isComparison(call.getKind()) || call.getOperands().size() != 2) {
            return null;
        }
        RexNode first = call.getOperands().get(0);
        RexNode second = call.getOperands().get(1);
        String op = operator(call.getOperator().getName());
        if (first instanceof RexInputRef && second instanceof RexInputRef) {
            int firstIndex = ((RexInputRef) first).getIndex();
            int secondIndex = ((RexInputRef) second).getIndex();
            if (!validInput(firstIndex, leftSize, rightFields.size())
                    || !validInput(secondIndex, leftSize, rightFields.size())) {
                return null;
            }
            if (firstIndex < leftSize && secondIndex >= leftSize) {
                return rightField(rightFields, secondIndex - leftSize) + " " + reverse(op) + " "
                        + formatValue(leftRow[firstIndex]);
            }
            if (secondIndex < leftSize && firstIndex >= leftSize) {
                return rightField(rightFields, firstIndex - leftSize) + " " + op + " "
                        + formatValue(leftRow[secondIndex]);
            }
            if (firstIndex >= leftSize && secondIndex >= leftSize) {
                return rightField(rightFields, firstIndex - leftSize) + " " + op + " "
                        + rightField(rightFields, secondIndex - leftSize);
            }
            return null;
        }
        if (first instanceof RexInputRef && second instanceof RexLiteral) {
            int index = ((RexInputRef) first).getIndex();
            return !rightInput(index, leftSize, rightFields.size()) ? null
                    : rightField(rightFields, index - leftSize) + " " + op
                            + " " + formatValue(((RexLiteral) second).getValue());
        }
        if (first instanceof RexLiteral && second instanceof RexInputRef) {
            int index = ((RexInputRef) second).getIndex();
            return !rightInput(index, leftSize, rightFields.size()) ? null
                    : formatValue(((RexLiteral) first).getValue()) + " "
                            + op + " " + rightField(rightFields, index - leftSize);
        }
        return null;
    }

    private static String buildJoinArray(
            RexCall call, Object[] leftRow, int leftSize, List<String> rightFields) {
        if (call.getOperands().size() != 2 || !(call.getOperands().get(0) instanceof RexInputRef)) {
            return null;
        }
        int arrayIndex = ((RexInputRef) call.getOperands().get(0)).getIndex();
        if (!rightInput(arrayIndex, leftSize, rightFields.size())) {
            return null;
        }
        RexNode valueNode = call.getOperands().get(1);
        if (requiresArrayArgument(call.getOperator().getName())) {
            boolean arrayValue = valueNode.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR);
            if (valueNode instanceof RexLiteral) {
                arrayValue = ((RexLiteral) valueNode).getValue() instanceof List;
            } else if (valueNode instanceof RexInputRef) {
                int valueIndex = ((RexInputRef) valueNode).getIndex();
                arrayValue = valueIndex >= 0 && valueIndex < leftSize
                        && leftRow[valueIndex] instanceof List;
            }
            if (!arrayValue) {
                return null;
            }
        }
        String value = joinValue(valueNode, leftRow, leftSize, rightFields);
        return value == null ? null : call.getOperator().getName().toUpperCase() + "("
                + rightField(rightFields, arrayIndex - leftSize) + ", " + value + ")";
    }

    private static String joinValue(
            RexNode node, Object[] leftRow, int leftSize, List<String> rightFields) {
        if (node instanceof RexLiteral) {
            return formatValue(((RexLiteral) node).getValue());
        }
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return index >= 0 && index < leftSize ? formatValue(leftRow[index]) : null;
        }
        if (node.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            List<String> values = new ArrayList<>();
            for (RexNode element : ((RexCall) node).getOperands()) {
                String value = joinValue(element, leftRow, leftSize, rightFields);
                if (value == null) {
                    return null;
                }
                values.add(value);
            }
            return "[" + String.join(", ", values) + "]";
        }
        return null;
    }

    /** Returns true only when the complete join predicate can be evaluated by Milvus. */
    public static boolean supportsJoinFilter(
            RexNode condition, int leftFieldCount, int rightFieldCount) {
        if (!(condition instanceof RexCall) || leftFieldCount < 0 || rightFieldCount < 0) {
            return false;
        }
        RexCall call = (RexCall) condition;
        if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR)) {
            return !call.getOperands().isEmpty() && call.getOperands().stream().allMatch(
                    operand -> supportsJoinFilter(operand, leftFieldCount, rightFieldCount));
        }
        if (isArrayOperator(call.getOperator().getName())) {
            return supportsJoinArray(call, leftFieldCount, rightFieldCount);
        }
        if (!isComparison(call.getKind()) || call.getOperands().size() != 2) {
            return false;
        }
        RexNode first = call.getOperands().get(0);
        RexNode second = call.getOperands().get(1);
        if (first instanceof RexInputRef && second instanceof RexInputRef) {
            int firstIndex = ((RexInputRef) first).getIndex();
            int secondIndex = ((RexInputRef) second).getIndex();
            return validInput(firstIndex, leftFieldCount, rightFieldCount)
                    && validInput(secondIndex, leftFieldCount, rightFieldCount)
                    && (firstIndex >= leftFieldCount || secondIndex >= leftFieldCount);
        }
        if (first instanceof RexInputRef && second instanceof RexLiteral) {
            return rightInput(((RexInputRef) first).getIndex(), leftFieldCount, rightFieldCount);
        }
        return first instanceof RexLiteral && second instanceof RexInputRef
                && rightInput(((RexInputRef) second).getIndex(), leftFieldCount, rightFieldCount);
    }

    private static boolean supportsJoinArray(
            RexCall call, int leftFieldCount, int rightFieldCount) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !rightInput(((RexInputRef) call.getOperands().get(0)).getIndex(),
                        leftFieldCount, rightFieldCount)) {
            return false;
        }
        RexNode value = call.getOperands().get(1);
        if (requiresArrayArgument(call.getOperator().getName())
                && !value.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)
                && value.getType().getSqlTypeName() != SqlTypeName.ARRAY) {
            return false;
        }
        if (value instanceof RexLiteral) {
            return true;
        }
        if (value instanceof RexInputRef) {
            int index = ((RexInputRef) value).getIndex();
            return index >= 0 && index < leftFieldCount;
        }
        return value.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)
                && ((RexCall) value).getOperands().stream().allMatch(element ->
                        element instanceof RexLiteral || element instanceof RexInputRef
                                && ((RexInputRef) element).getIndex() >= 0
                                && ((RexInputRef) element).getIndex() < leftFieldCount);
    }

    private static boolean isArrayOperator(String operator) {
        return operator != null && ARRAY_OPERATORS.contains(operator.toLowerCase(Locale.ROOT));
    }

    private static boolean requiresArrayArgument(String operator) {
        return operator != null
                && !"array_contains".equals(operator.toLowerCase(Locale.ROOT));
    }

    private static boolean validInput(int index, int leftCount, int rightCount) {
        return index >= 0 && index < leftCount + rightCount;
    }

    private static boolean rightInput(int index, int leftCount, int rightCount) {
        return index >= leftCount && index < leftCount + rightCount;
    }

    private static boolean isComparison(SqlKind kind) {
        return kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS
                || kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL
                || kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
    }

    private static String rightField(List<String> fields, int index) {
        return identifier(fields.get(index));
    }

    private static String identifier(String name) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid Milvus field name: " + name);
        }
        return name;
    }

    private static String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof NlsString) {
            return quote(((NlsString) value).getValue());
        }
        if (value instanceof String) {
            return quote((String) value);
        }
        if (value instanceof List) {
            return "[" + ((List<?>) value).stream()
                    .map(MilvusFilterBuilder::formatValue)
                    .collect(Collectors.joining(", ")) + "]";
        }
        return value.toString();
    }

    private static String quote(String value) {
        StringBuilder result = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\': result.append("\\\\"); break;
                case '"': result.append("\\\""); break;
                case '\b': result.append("\\b"); break;
                case '\f': result.append("\\f"); break;
                case '\n': result.append("\\n"); break;
                case '\r': result.append("\\r"); break;
                case '\t': result.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        result.append(String.format(Locale.ROOT, "\\u%04x", (int) ch));
                    } else {
                        result.append(ch);
                    }
            }
        }
        return result.append('"').toString();
    }

    private static String operator(String operator) {
        if ("=".equals(operator)) {
            return "==";
        }
        return "<>".equals(operator) ? "!=" : operator;
    }

    private static String reverse(String operator) {
        switch (operator) {
            case ">": return "<";
            case "<": return ">";
            case ">=": return "<=";
            case "<=": return ">=";
            default: return operator;
        }
    }
}
