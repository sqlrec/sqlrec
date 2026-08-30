package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FilterUtils {

    /** Per-request build state: dialect, JDBC url (SQL dialect) and collected parameters. */
    private static final class FilterContext {
        final FilterDialect dialect;
        final String url;
        final List<Object> parameters = new ArrayList<>();

        FilterContext(FilterDialect dialect, String url) {
            this.dialect = dialect;
            this.url = url;
        }
    }

    private enum FilterDialect {
        MILVUS {
            @Override
            String renderLiteral(RexLiteral literal, FilterContext ctx) {
                Object value = literal.getValue();
                if (value == null) {
                    return "null";
                }
                if (value instanceof NlsString) {
                    return milvusQuote(((NlsString) value).getValue());
                }
                return value.toString();
            }

            @Override
            String renderField(String fieldName, FilterContext ctx) {
                return milvusIdentifier(fieldName);
            }

            @Override
            String formatArrayConstructor(String elements) {
                return "[" + elements + "]";
            }

            @Override
            String formatOperator(String operator) {
                return "=".equals(operator) ? "==" : operator;
            }

            @Override
            String formatArrayContains(String arrayField, String elementValue) {
                return "array_contains(" + arrayField + ", " + elementValue + ")";
            }
        },
        SQL {
            @Override
            String renderLiteral(RexLiteral literal, FilterContext ctx) {
                // Bind the value as a JDBC parameter instead of interpolating it into the
                // SQL text. Escaping rules for string literals differ per database (and
                // per sql_mode), so parameterization is the only injection-proof approach.
                // getValue3() keeps decimals as BigDecimal (getValue2() would truncate
                // them to Long) and converts char literals to String.
                ctx.parameters.add(literal.getValue3());
                return "?";
            }

            @Override
            String renderField(String fieldName, FilterContext ctx) {
                // Field names come from (possibly untrusted) table metadata and are
                // inlined into the WHERE clause; quote unsafe identifiers the same way
                // the SELECT column list does.
                return SqlUtils.quoteIdentifier(fieldName, ctx.url);
            }

            @Override
            String formatArrayConstructor(String elements) {
                return elements;
            }

            @Override
            String formatOperator(String operator) {
                return operator;
            }

            @Override
            String formatArrayContains(String arrayField, String elementValue) {
                return arrayField + " @> ARRAY[" + elementValue + "]";
            }
        };

        abstract String renderLiteral(RexLiteral literal, FilterContext ctx);
        abstract String renderField(String fieldName, FilterContext ctx);
        abstract String formatArrayConstructor(String elements);
        abstract String formatOperator(String operator);
        abstract String formatArrayContains(String arrayField, String elementValue);
    }

    /**
     * Quote a string literal for a Milvus filter expression, escaping backslash and double-quote so
     * that values containing {@code "} or {@code \} cannot break the expression syntax or inject
     * additional filter clauses.
     */
    private static String milvusQuote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * Milvus filter expressions have no identifier quoting mechanism, so a field name containing
     * special characters could inject expression syntax (e.g. {@code a" or "1" == "1}). Only
     * names matching Milvus's own identifier rules are accepted; anything else fails fast.
     */
    private static final Pattern MILVUS_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private static String milvusIdentifier(String fieldName) {
        if (fieldName == null || !MILVUS_IDENTIFIER.matcher(fieldName).matches()) {
            throw new IllegalArgumentException("Invalid Milvus field name: " + fieldName);
        }
        return fieldName;
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
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            RexInputRef inputRef = (RexInputRef) left;
            if (inputRef.getIndex() == primaryKeyIndex) {
                return ((RexLiteral) right).getValue2();
            }
        } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
            RexInputRef inputRef = (RexInputRef) right;
            if (inputRef.getIndex() == primaryKeyIndex) {
                return ((RexLiteral) left).getValue2();
            }
        }
        return null;
    }

    public static List<RexNode> getPrimaryKeyFilters(List<RexNode> filters, int primaryKeyIndex) {
        for (RexNode filter : filters) {
            if (filter.isA(SqlKind.EQUALS)) {
                RexCall call = (RexCall) filter;
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexInputRef) {
                        RexInputRef inputRef = (RexInputRef) operand;
                        if (inputRef.getIndex() == primaryKeyIndex) {
                            return Collections.singletonList(call);
                        }
                    }
                }
            } else if (filter.isA(SqlKind.AND)) {
                RexCall call = (RexCall) filter;
                List<RexNode> kvTableFilters = getPrimaryKeyFilters(call.getOperands(), primaryKeyIndex);
                if (kvTableFilters != null && !kvTableFilters.isEmpty()) {
                    return kvTableFilters;
                }
            }
        }
        return Collections.emptyList();
    }

    // --- Unified filter string generation ---

    public static String getMilvusFilterSqlString(List<RexNode> filters, List<FieldSchema> fieldSchemas) {
        return getFilterString(filters, fieldSchemas, new FilterContext(FilterDialect.MILVUS, null));
    }

    /**
     * Build a parameterized SQL WHERE clause fragment from Calcite filter conditions.
     * Literal values are bound as JDBC parameters (one per {@code '?'} placeholder, in
     * order) instead of being interpolated into the SQL text, which makes the fragment
     * immune to string-literal injection regardless of how the target database
     * interprets escapes inside string literals (e.g. MySQL by default treats {@code '\'}
     * as an escape character, which defeats single-quote doubling).
     *
     * @param filters      Calcite filter conditions (may be {@code null} or empty)
     * @param fieldSchemas field schemas used to resolve {@code RexInputRef} indexes to field names
     * @param url          JDBC url, used to pick the identifier quoting style (MySQL vs ANSI)
     * @return WHERE clause fragment ({@link SqlStatement#getSql()} is empty when no
     *         filters apply) plus the parameter values bound to its placeholders
     */
    public static SqlStatement buildSqlFilter(List<RexNode> filters, List<FieldSchema> fieldSchemas, String url) {
        FilterContext ctx = new FilterContext(FilterDialect.SQL, url);
        String expression = getFilterString(filters, fieldSchemas, ctx);
        return new SqlStatement(expression, ctx.parameters);
    }

    private static String getFilterString(List<RexNode> filters, List<FieldSchema> fieldSchemas, FilterContext ctx) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        List<String> expressions = new ArrayList<>(filters.size());
        for (RexNode filter : filters) {
            String expression = getFilterString(filter, fieldSchemas, ctx);
            // An unsupported Milvus predicate must remain a residual Calcite filter;
            // never stringify Java null into a Milvus expression.
            if (expression == null && ctx.dialect == FilterDialect.MILVUS) {
                return null;
            }
            expressions.add(expression);
        }
        return expressions.stream().collect(Collectors.joining(" AND "));
    }

    private static String getFilterString(RexNode filter, List<FieldSchema> fieldSchemas, FilterContext ctx) {
        if (filter.isA(SqlKind.OR)) {
            RexCall call = (RexCall) filter;
            return call.getOperands().stream()
                    .map(operand -> "(" + getFilterString(operand, fieldSchemas, ctx) + ")")
                    .collect(Collectors.joining(" OR "));
        } else if (filter.isA(SqlKind.AND)) {
            RexCall call = (RexCall) filter;
            return call.getOperands().stream()
                    .map(operand -> "(" + getFilterString(operand, fieldSchemas, ctx) + ")")
                    .collect(Collectors.joining(" AND "));
        }
        RexCall call = (RexCall) filter;
        return convertNormalFilter(call, fieldSchemas, ctx);
    }

    private static String convertNormalFilter(RexCall filter, List<FieldSchema> fieldSchemas, FilterContext ctx) {
        String operator = filter.getOperator().getName();

        if (operator.toLowerCase().startsWith("array_contains")) {
            return convertArrayFunctionFilter(filter, fieldSchemas, operator.toLowerCase(), ctx);
        }

        if (filter.isA(SqlKind.SEARCH)) {
            return convertSearchFilter(filter, fieldSchemas, ctx);
        }

        if (filter.getOperands().size() == 1) {
            String field = convertOperand(filter.getOperands().get(0), fieldSchemas, ctx);
            switch (filter.getKind()) {
                case IS_NULL:
                case IS_UNKNOWN:
                    return ctx.dialect == FilterDialect.SQL ? field + " IS NULL" : null;
                case IS_NOT_NULL:
                    return ctx.dialect == FilterDialect.SQL ? field + " IS NOT NULL" : null;
                case NOT:
                    return ctx.dialect == FilterDialect.SQL ? "NOT (" + field + ")" : null;
                default:
                    break;
            }
        }

        if (filter.getOperands().size() != 2) {
            throw new IllegalArgumentException("Unsupported filter: " + filter);
        }

        String firstOperand = convertOperand(filter.getOperands().get(0), fieldSchemas, ctx);
        String secondOperand = convertOperand(filter.getOperands().get(1), fieldSchemas, ctx);
        return firstOperand + " " + ctx.dialect.formatOperator(operator) + " " + secondOperand;
    }

    private static String convertSearchFilter(RexCall filter, List<FieldSchema> fieldSchemas, FilterContext ctx) {
        if (filter.getOperands().size() != 2
                || !(filter.getOperands().get(0) instanceof RexInputRef)
                || !(filter.getOperands().get(1) instanceof RexLiteral)) {
            throw new IllegalArgumentException("Unsupported SEARCH filter: " + filter);
        }

        String field = convertOperand(filter.getOperands().get(0), fieldSchemas, ctx);
        Sarg<?> sarg = ((RexLiteral) filter.getOperands().get(1)).getValueAs(Sarg.class);
        if (sarg == null || (!sarg.isPoints() && !sarg.isComplementedPoints())) {
            throw new IllegalArgumentException("Unsupported SEARCH range: " + filter);
        }

        List<String> values = new ArrayList<>();
        for (Range<?> range : sarg.rangeSet.asRanges()) {
            if (!range.hasLowerBound() || !range.hasUpperBound()
                    || range.lowerBoundType() != com.google.common.collect.BoundType.CLOSED
                    || range.upperBoundType() != com.google.common.collect.BoundType.CLOSED
                    || !range.lowerEndpoint().equals(range.upperEndpoint())) {
                throw new IllegalArgumentException("Unsupported SEARCH point range: " + filter);
            }
            if (ctx.dialect == FilterDialect.SQL) {
                ctx.parameters.add(range.lowerEndpoint());
                values.add("?");
            } else {
                values.add(formatMilvusSargValue(range.lowerEndpoint()));
            }
        }
        if (values.isEmpty()) {
            return ctx.dialect == FilterDialect.SQL ? "1 = 0" : "false";
        }
        String operator = sarg.isComplementedPoints() ? " NOT IN " : " IN ";
        String joined = String.join(", ", values);
        String open = ctx.dialect == FilterDialect.SQL ? "(" : "[";
        String close = ctx.dialect == FilterDialect.SQL ? ")" : "]";
        return field + operator + open + joined + close;
    }

    private static String formatMilvusSargValue(Object value) {
        if (value instanceof NlsString) {
            return milvusQuote(((NlsString) value).getValue());
        }
        return formatValue(value);
    }

    private static String convertArrayFunctionFilter(RexCall filter, List<FieldSchema> fieldSchemas, String functionName, FilterContext ctx) {
        if (filter.getOperands().size() != 2) {
            throw new IllegalArgumentException(functionName + " requires exactly 2 arguments");
        }

        String arrayField = convertOperand(filter.getOperands().get(0), fieldSchemas, ctx);
        String elementValue = convertOperand(filter.getOperands().get(1), fieldSchemas, ctx);

        if ("array_contains".equals(functionName)) {
            return ctx.dialect.formatArrayContains(arrayField, elementValue);
        }
        return functionName + "(" + arrayField + ", " + elementValue + ")";
    }

    private static String convertOperand(RexNode operand, List<FieldSchema> fieldSchemas, FilterContext ctx) {
        if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;
            String fieldName = fieldSchemas.get(inputRef.getIndex()).getName();
            return ctx.dialect.renderField(fieldName, ctx);
        }
        if (operand instanceof RexLiteral) {
            return ctx.dialect.renderLiteral((RexLiteral) operand, ctx);
        }
        if (operand instanceof RexCall) {
            RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                String elements = call.getOperands().stream()
                        .map(elem -> convertOperand(elem, fieldSchemas, ctx))
                        .collect(Collectors.joining(", "));
                return ctx.dialect.formatArrayConstructor(elements);
            }
        }
        throw new IllegalArgumentException("Unsupported operand kind: " + operand.getKind());
    }

    // --- Milvus join filter expression ---

    public static String buildMilvusFilterExpression(
            RexNode filterCondition,
            Object[] leftValue,
            List<String> rightFieldNames) {

        if (filterCondition == null) {
            return null;
        }

        // Field names are inlined into the Milvus expression and Milvus filter syntax has
        // no identifier quoting, so only Milvus-legal identifiers are accepted up-front.
        if (rightFieldNames != null) {
            for (String fieldName : rightFieldNames) {
                milvusIdentifier(fieldName);
            }
        }

        int leftSize = leftValue != null ? leftValue.length : 0;

        return buildFilterExpressionRecursive(
                filterCondition,
                leftValue,
                leftSize,
                rightFieldNames
        );
    }

    /**
     * Returns whether a join filter can be represented completely by Milvus.
     * A partial predicate must never be pushed because filtering after ANN top-K
     * can return fewer or different rows.
     */
    public static boolean canBuildMilvusJoinFilter(
            RexNode filterCondition,
            int leftFieldCount,
            int rightFieldCount) {
        if (filterCondition == null || leftFieldCount < 0 || rightFieldCount < 0) {
            return false;
        }
        if (!(filterCondition instanceof RexCall)) {
            return false;
        }

        RexCall call = (RexCall) filterCondition;
        if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR)) {
            return !call.getOperands().isEmpty()
                    && call.getOperands().stream().allMatch(
                            operand -> canBuildMilvusJoinFilter(
                                    operand, leftFieldCount, rightFieldCount));
        }
        if (call.getOperator().getName().toLowerCase().startsWith("array_contains")) {
            return canBuildMilvusArrayJoinFilter(call, leftFieldCount, rightFieldCount);
        }
        if (!isMilvusComparison(call.getKind()) || call.getOperands().size() != 2) {
            return false;
        }

        RexNode first = call.getOperands().get(0);
        RexNode second = call.getOperands().get(1);
        if (first instanceof RexInputRef && second instanceof RexInputRef) {
            int firstIndex = ((RexInputRef) first).getIndex();
            int secondIndex = ((RexInputRef) second).getIndex();
            return isValidJoinInput(firstIndex, leftFieldCount, rightFieldCount)
                    && isValidJoinInput(secondIndex, leftFieldCount, rightFieldCount)
                    && (firstIndex >= leftFieldCount || secondIndex >= leftFieldCount);
        }
        if (first instanceof RexInputRef && second instanceof RexLiteral) {
            return isRightInput(
                    ((RexInputRef) first).getIndex(), leftFieldCount, rightFieldCount);
        }
        if (first instanceof RexLiteral && second instanceof RexInputRef) {
            return isRightInput(
                    ((RexInputRef) second).getIndex(), leftFieldCount, rightFieldCount);
        }
        return false;
    }

    private static boolean canBuildMilvusArrayJoinFilter(
            RexCall call,
            int leftFieldCount,
            int rightFieldCount) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !isRightInput(
                        ((RexInputRef) call.getOperands().get(0)).getIndex(),
                        leftFieldCount,
                        rightFieldCount)) {
            return false;
        }

        RexNode value = call.getOperands().get(1);
        if (value instanceof RexLiteral) {
            return true;
        }
        if (value instanceof RexInputRef) {
            return isValidJoinInput(
                    ((RexInputRef) value).getIndex(), leftFieldCount, rightFieldCount);
        }
        if (!(value instanceof RexCall)
                || !value.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            return false;
        }
        for (RexNode element : ((RexCall) value).getOperands()) {
            if (element instanceof RexLiteral) {
                continue;
            }
            if (!(element instanceof RexInputRef)
                    || !isValidJoinInput(
                            ((RexInputRef) element).getIndex(),
                            leftFieldCount,
                            rightFieldCount)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidJoinInput(
            int index, int leftFieldCount, int rightFieldCount) {
        return index >= 0 && index < leftFieldCount + rightFieldCount;
    }

    private static boolean isRightInput(
            int index, int leftFieldCount, int rightFieldCount) {
        return index >= leftFieldCount
                && index < leftFieldCount + rightFieldCount;
    }

    private static String buildFilterExpressionRecursive(
            RexNode node,
            Object[] leftValue,
            int leftSize,
            List<String> rightFieldNames) {

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            String opName = call.getOperator().getName();

            if (opName.equalsIgnoreCase("AND") || opName.equalsIgnoreCase("OR")) {
                String joiner = opName.equalsIgnoreCase("AND") ? " and " : " or ";
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < call.getOperands().size(); i++) {
                    String operandExpression = buildFilterExpressionRecursive(
                            call.getOperands().get(i), leftValue, leftSize, rightFieldNames);
                    // A compound expression is pushable only when every child is
                    // representable. Partial expressions can change join results.
                    if (operandExpression == null) {
                        return null;
                    }
                    if (i > 0) sb.append(joiner);
                    sb.append(operandExpression);
                }
                sb.append(")");
                return sb.toString();
            }

            if (opName.toLowerCase().startsWith("array_contains")) {
                return buildArrayFunctionFilterExpression(call, opName, leftValue, leftSize, rightFieldNames);
            }

            if (!isMilvusComparison(call.getKind())) {
                return null;
            }

            if (call.getOperands().size() == 2) {
                RexNode leftOperand = call.getOperands().get(0);
                RexNode rightOperand = call.getOperands().get(1);

                if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {
                    int leftIdx = ((RexInputRef) leftOperand).getIndex();
                    int rightIdx = ((RexInputRef) rightOperand).getIndex();

                    String fieldName;
                    Object value;
                    String op;

                    if (leftIdx < leftSize && rightIdx >= leftSize) {
                        fieldName = rightFieldNames.get(rightIdx - leftSize);
                        value = leftValue[leftIdx];
                        op = getOperator(opName);
                    } else if (rightIdx < leftSize && leftIdx >= leftSize) {
                        fieldName = rightFieldNames.get(leftIdx - leftSize);
                        value = leftValue[rightIdx];
                        op = reverseOperator(getOperator(opName));
                    } else if (leftIdx >= leftSize && rightIdx >= leftSize) {
                        String leftFieldName = rightFieldNames.get(leftIdx - leftSize);
                        String rightFieldName = rightFieldNames.get(rightIdx - leftSize);
                        return leftFieldName + " " + getOperator(opName) + " " + rightFieldName;
                    } else {
                        return null;
                    }

                    return fieldName + " " + op + " " + formatValue(value);
                }

                if (leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
                    int idx = ((RexInputRef) leftOperand).getIndex();
                    if (idx >= leftSize) {
                        String fieldName = rightFieldNames.get(idx - leftSize);
                        Object value = ((RexLiteral) rightOperand).getValue();
                        return fieldName + " " + getOperator(opName) + " " + formatValue(value);
                    }
                }
                if (leftOperand instanceof RexLiteral && rightOperand instanceof RexInputRef) {
                    int idx = ((RexInputRef) rightOperand).getIndex();
                    if (idx >= leftSize) {
                        String fieldName = rightFieldNames.get(idx - leftSize);
                        Object value = ((RexLiteral) leftOperand).getValue();
                        return formatValue(value) + " " + getOperator(opName) + " " + fieldName;
                    }
                }
            }
        }

        return null;
    }

    private static boolean isMilvusComparison(SqlKind kind) {
        switch (kind) {
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private static String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof NlsString) {
            return milvusQuote(((NlsString) value).getValue());
        }
        if (value instanceof String) {
            return milvusQuote((String) value);
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(formatValue(list.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
        return value.toString();
    }

    private static String getOperator(String op) {
        return "=".equals(op) ? "==" : op;
    }

    private static String reverseOperator(String op) {
        switch (op) {
            case ">":
                return "<";
            case "<":
                return ">";
            case ">=":
                return "<=";
            case "<=":
                return ">=";
            default:
                return op;
        }
    }

    private static String buildArrayFunctionFilterExpression(
            RexCall call,
            String opName,
            Object[] leftValue,
            int leftSize,
            List<String> rightFieldNames) {

        if (call.getOperands().size() != 2) {
            return null;
        }

        RexNode arrayFieldOperand = call.getOperands().get(0);
        RexNode elementOperand = call.getOperands().get(1);

        String arrayField = null;
        String elementValue = null;

        if (arrayFieldOperand instanceof RexInputRef) {
            int idx = ((RexInputRef) arrayFieldOperand).getIndex();
            if (idx >= leftSize) {
                arrayField = rightFieldNames.get(idx - leftSize);
            }
        }

        if (elementOperand instanceof RexInputRef) {
            int idx = ((RexInputRef) elementOperand).getIndex();
            if (idx < leftSize && leftValue != null) {
                elementValue = formatValue(leftValue[idx]);
            } else if (idx >= leftSize) {
                elementValue = rightFieldNames.get(idx - leftSize);
            }
        } else if (elementOperand instanceof RexLiteral) {
            Object value = ((RexLiteral) elementOperand).getValue();
            elementValue = formatValue(value);
        } else if (elementOperand instanceof RexCall) {
            RexCall elementCall = (RexCall) elementOperand;
            if (elementCall.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < elementCall.getOperands().size(); i++) {
                    if (i > 0) sb.append(", ");
                    RexNode elem = elementCall.getOperands().get(i);
                    if (elem instanceof RexInputRef) {
                        int idx = ((RexInputRef) elem).getIndex();
                        if (idx < leftSize && leftValue != null) {
                            sb.append(formatValue(leftValue[idx]));
                        } else if (idx >= leftSize) {
                            sb.append(rightFieldNames.get(idx - leftSize));
                        }
                    } else if (elem instanceof RexLiteral) {
                        Object value = ((RexLiteral) elem).getValue();
                        sb.append(formatValue(value));
                    }
                }
                sb.append("]");
                elementValue = sb.toString();
            }
        }

        if (arrayField != null && elementValue != null) {
            return opName.toUpperCase() + "(" + arrayField + ", " + elementValue + ")";
        }

        return null;
    }
}
