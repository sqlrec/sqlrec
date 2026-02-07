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
        String firstOperand = convertOperand(filter.getOperands().get(0), fieldSchemas);
        String secondOperand = convertOperand(filter.getOperands().get(1), fieldSchemas);
        String operator = filter.getOperator().getName();
        if (operator.equals("=")) {
            operator = "==";
        }
        return firstOperand + " " + operator + " " + secondOperand;
    }

    public static String convertOperand(RexNode operand, List<FieldSchema> fieldSchemas) {
        if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;
            return fieldSchemas.get(inputRef.getIndex()).name;
        }
        if (operand instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) operand;
            Object value = literal.getValue();
            if (value instanceof NlsString) {
                return "\"" + ((NlsString) value).getValue() + "\"";
            }
            return value.toString();
        }
        throw new IllegalArgumentException("Unsupported operand kind: " + operand.getKind());
    }
}
