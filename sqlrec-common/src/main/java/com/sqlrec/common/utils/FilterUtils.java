package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FilterUtils {
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

    public static String getMilvusFilterSqlString(List<RexNode> filters, List<FieldSchema> fieldSchemas) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        return filters.stream()
                .map(filter -> getMilvusFilterSqlString(filter, fieldSchemas))
                .collect(Collectors.joining(" AND "));
    }

    public static String getMilvusFilterSqlString(RexNode filter, List<FieldSchema> fieldSchemas) {
        if (filter.isA(SqlKind.OR)) {
            RexCall call = (RexCall) filter;
            return call.getOperands().stream()
                    .map(operand -> "(" + getMilvusFilterSqlString(operand, fieldSchemas) + ")")
                    .collect(Collectors.joining(" OR "));
        } else if (filter.isA(SqlKind.AND)) {
            RexCall call = (RexCall) filter;
            return call.getOperands().stream()
                    .map(operand -> "(" + getMilvusFilterSqlString(operand, fieldSchemas) + ")")
                    .collect(Collectors.joining(" AND "));
        }

        RexCall call = (RexCall) filter;
        return convertNormalFilter(call, fieldSchemas);
    }

    public static String convertNormalFilter(RexCall filter, List<FieldSchema> fieldSchemas) {
        String operator = filter.getOperator().getName();

        if (operator.toLowerCase().startsWith("array_contains")) {
            return convertArrayFunctionFilter(filter, fieldSchemas, operator.toLowerCase());
        }

        if (filter.getOperands().size() != 2) {
            throw new IllegalArgumentException("Unsupported filter: " + filter);
        }

        String firstOperand = convertOperand(filter.getOperands().get(0), fieldSchemas);
        String secondOperand = convertOperand(filter.getOperands().get(1), fieldSchemas);
        if (operator.equals("=")) {
            operator = "==";
        }
        return firstOperand + " " + operator + " " + secondOperand;
    }

    private static String convertArrayFunctionFilter(RexCall filter, List<FieldSchema> fieldSchemas, String functionName) {
        if (filter.getOperands().size() != 2) {
            throw new IllegalArgumentException(functionName + " requires exactly 2 arguments");
        }

        String arrayField = convertOperand(filter.getOperands().get(0), fieldSchemas);
        String elementValue = convertOperand(filter.getOperands().get(1), fieldSchemas);

        return functionName + "(" + arrayField + ", " + elementValue + ")";
    }

    public static String convertOperand(RexNode operand, List<FieldSchema> fieldSchemas) {
        if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;
            return fieldSchemas.get(inputRef.getIndex()).getName();
        }
        if (operand instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) operand;
            Object value = literal.getValue();
            if (value instanceof NlsString) {
                return "\"" + ((NlsString) value).getValue() + "\"";
            }
            return value.toString();
        }
        if (operand instanceof RexCall) {
            RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                String elements = call.getOperands().stream()
                        .map(elem -> convertOperand(elem, fieldSchemas))
                        .collect(Collectors.joining(", "));
                return "[" + elements + "]";
            }
        }
        throw new IllegalArgumentException("Unsupported operand kind: " + operand.getKind());
    }

    public static String buildMilvusFilterExpression(
            RexNode filterCondition,
            Object[] leftValue,
            List<String> rightFieldNames) {

        if (filterCondition == null) {
            return null;
        }

        int leftSize = leftValue != null ? leftValue.length : 0;

        return buildFilterExpressionRecursive(
                filterCondition,
                leftValue,
                leftSize,
                rightFieldNames
        );
    }

    private static String buildFilterExpressionRecursive(
            RexNode node,
            Object[] leftValue,
            int leftSize,
            List<String> rightFieldNames) {

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            String opName = call.getOperator().getName();

            if (opName.equalsIgnoreCase("AND")) {
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < call.getOperands().size(); i++) {
                    if (i > 0) sb.append(" and ");
                    sb.append(buildFilterExpressionRecursive(call.getOperands().get(i), leftValue, leftSize, rightFieldNames));
                }
                sb.append(")");
                return sb.toString();
            }
            if (opName.equalsIgnoreCase("OR")) {
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < call.getOperands().size(); i++) {
                    if (i > 0) sb.append(" or ");
                    sb.append(buildFilterExpressionRecursive(call.getOperands().get(i), leftValue, leftSize, rightFieldNames));
                }
                sb.append(")");
                return sb.toString();
            }

            if (opName.toLowerCase().startsWith("array_contains")) {
                return buildArrayFunctionFilterExpression(call, opName, leftValue, leftSize, rightFieldNames);
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

    private static String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof NlsString) {
            return "\"" + ((NlsString) value).getValue() + "\"";
        }
        if (value instanceof String) {
            return "\"" + value + "\"";
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
        switch (op) {
            case "=":
                return "==";
            default:
                return op;
        }
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
