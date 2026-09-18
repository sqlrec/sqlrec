package com.sqlrec.connectors.milvus.filter;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Translates Calcite predicates to Milvus filter expressions. */
public final class MilvusFilterBuilder {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private MilvusFilterBuilder() {
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
        if (operator.toLowerCase().startsWith("array_contains")) {
            if (call.getOperands().size() != 2) {
                throw new IllegalArgumentException(operator + " requires exactly 2 arguments");
            }
            return operator.toLowerCase() + "(" + scanOperand(call.getOperands().get(0), fields)
                    + ", " + scanOperand(call.getOperands().get(1), fields) + ")";
        }
        if (call.isA(SqlKind.SEARCH)) {
            return buildSearch(call, fields);
        }
        if (call.getOperands().size() == 1) {
            return null;
        }
        if (call.getOperands().size() != 2 || !isComparison(call.getKind())) {
            throw new IllegalArgumentException("Unsupported Milvus filter: " + call);
        }
        return scanOperand(call.getOperands().get(0), fields) + " "
                + operator(operator) + " " + scanOperand(call.getOperands().get(1), fields);
    }

    private static String buildSearch(RexCall call, List<FieldSchema> fields) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !(call.getOperands().get(1) instanceof RexLiteral)) {
            throw new IllegalArgumentException("Unsupported SEARCH filter: " + call);
        }
        Sarg<?> sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
        if (sarg == null || (!sarg.isPoints() && !sarg.isComplementedPoints())) {
            throw new IllegalArgumentException("Unsupported SEARCH range: " + call);
        }
        List<String> values = new ArrayList<>();
        for (Range<?> range : sarg.rangeSet.asRanges()) {
            if (!range.hasLowerBound() || !range.hasUpperBound()
                    || range.lowerBoundType() != BoundType.CLOSED
                    || range.upperBoundType() != BoundType.CLOSED
                    || !range.lowerEndpoint().equals(range.upperEndpoint())) {
                throw new IllegalArgumentException("Unsupported SEARCH point range: " + call);
            }
            values.add(formatValue(range.lowerEndpoint()));
        }
        if (values.isEmpty()) {
            return "false";
        }
        return scanOperand(call.getOperands().get(0), fields)
                + (sarg.isComplementedPoints() ? " NOT IN [" : " IN [")
                + String.join(", ", values) + "]";
    }

    private static String scanOperand(RexNode node, List<FieldSchema> fields) {
        if (node instanceof RexInputRef) {
            return identifier(fields.get(((RexInputRef) node).getIndex()).getName());
        }
        if (node instanceof RexLiteral) {
            return formatValue(((RexLiteral) node).getValue());
        }
        if (node.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            return "[" + ((RexCall) node).getOperands().stream()
                    .map(operand -> scanOperand(operand, fields))
                    .collect(Collectors.joining(", ")) + "]";
        }
        throw new IllegalArgumentException("Unsupported Milvus operand: " + node);
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
        if (call.getOperator().getName().toLowerCase().startsWith("array_contains")) {
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
            return index < leftSize ? null : rightField(rightFields, index - leftSize) + " " + op
                    + " " + formatValue(((RexLiteral) second).getValue());
        }
        if (first instanceof RexLiteral && second instanceof RexInputRef) {
            int index = ((RexInputRef) second).getIndex();
            return index < leftSize ? null : formatValue(((RexLiteral) first).getValue()) + " "
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
        if (arrayIndex < leftSize) {
            return null;
        }
        String value = joinValue(call.getOperands().get(1), leftRow, leftSize, rightFields);
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
            return index < leftSize ? formatValue(leftRow[index])
                    : rightField(rightFields, index - leftSize);
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
        if (call.getOperator().getName().toLowerCase().startsWith("array_contains")) {
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
        if (value instanceof RexLiteral) {
            return true;
        }
        if (value instanceof RexInputRef) {
            return validInput(((RexInputRef) value).getIndex(), leftFieldCount, rightFieldCount);
        }
        return value.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)
                && ((RexCall) value).getOperands().stream().allMatch(element ->
                        element instanceof RexLiteral || element instanceof RexInputRef
                                && validInput(((RexInputRef) element).getIndex(),
                                        leftFieldCount, rightFieldCount));
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
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String operator(String operator) {
        return "=".equals(operator) ? "==" : operator;
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
