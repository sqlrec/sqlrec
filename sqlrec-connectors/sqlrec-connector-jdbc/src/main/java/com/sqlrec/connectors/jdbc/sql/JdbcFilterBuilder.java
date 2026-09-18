package com.sqlrec.connectors.jdbc.sql;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Translates Calcite predicates into parameterized JDBC SQL fragments. */
public final class JdbcFilterBuilder {
    private JdbcFilterBuilder() {
    }

    /** Builds a JDBC SQL fragment whose literal values are represented by parameters. */
    public static JdbcStatement build(
            List<RexNode> filters, List<FieldSchema> fieldSchemas, String url) {
        SqlFilterBuilder builder = new SqlFilterBuilder(fieldSchemas, url);
        return new JdbcStatement(builder.build(filters), builder.parameters);
    }

    private static final class SqlFilterBuilder {
        private final List<FieldSchema> fields;
        private final String url;
        private final List<Object> parameters = new ArrayList<>();

        private SqlFilterBuilder(List<FieldSchema> fields, String url) {
            this.fields = fields;
            this.url = url;
        }

        private String build(List<RexNode> filters) {
            if (filters == null || filters.isEmpty()) {
                return "";
            }
            return filters.stream().map(this::build).collect(Collectors.joining(" AND "));
        }

        private String build(RexNode node) {
            RexCall call = (RexCall) node;
            if (call.isA(SqlKind.AND) || call.isA(SqlKind.OR)) {
                String delimiter = call.isA(SqlKind.AND) ? " AND " : " OR ";
                return call.getOperands().stream()
                        .map(this::build)
                        .map(expression -> "(" + expression + ")")
                        .collect(Collectors.joining(delimiter));
            }
            String operator = call.getOperator().getName();
            if (operator.toLowerCase().startsWith("array_contains")) {
                return buildArrayFunction(call, operator.toLowerCase());
            }
            if (call.isA(SqlKind.SEARCH)) {
                return buildSearch(call);
            }
            if (call.getOperands().size() == 1) {
                String operand = operand(call.getOperands().get(0));
                switch (call.getKind()) {
                    case IS_NULL:
                    case IS_UNKNOWN:
                        return operand + " IS NULL";
                    case IS_NOT_NULL:
                        return operand + " IS NOT NULL";
                    case NOT:
                        return "NOT (" + operand + ")";
                    default:
                        break;
                }
            }
            if (call.getOperands().size() != 2) {
                throw new IllegalArgumentException("Unsupported filter: " + call);
            }
            return operand(call.getOperands().get(0)) + " " + operator + " "
                    + operand(call.getOperands().get(1));
        }

        private String buildSearch(RexCall call) {
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
                parameters.add(range.lowerEndpoint());
                values.add("?");
            }
            if (values.isEmpty()) {
                return "1 = 0";
            }
            return operand(call.getOperands().get(0))
                    + (sarg.isComplementedPoints() ? " NOT IN (" : " IN (")
                    + String.join(", ", values) + ")";
        }

        private String buildArrayFunction(RexCall call, String functionName) {
            if (call.getOperands().size() != 2) {
                throw new IllegalArgumentException(functionName + " requires exactly 2 arguments");
            }
            String array = operand(call.getOperands().get(0));
            String value = operand(call.getOperands().get(1));
            return "array_contains".equals(functionName)
                    ? array + " @> ARRAY[" + value + "]"
                    : functionName + "(" + array + ", " + value + ")";
        }

        private String operand(RexNode node) {
            if (node instanceof RexInputRef) {
                String name = fields.get(((RexInputRef) node).getIndex()).getName();
                return JdbcSqlBuilder.quoteIdentifier(name, url);
            }
            if (node instanceof RexLiteral) {
                parameters.add(((RexLiteral) node).getValue3());
                return "?";
            }
            if (node.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
                return ((RexCall) node).getOperands().stream()
                        .map(this::operand)
                        .collect(Collectors.joining(", "));
            }
            throw new IllegalArgumentException("Unsupported operand kind: " + node.getKind());
        }
    }
}

