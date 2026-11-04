package com.sqlrec.connectors.jdbc.sql;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

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
            List<String> expressions = new ArrayList<>();
            for (RexNode filter : filters) {
                int parameterCount = parameters.size();
                try {
                    expressions.add(build(filter));
                } catch (UnsupportedOperationException e) {
                    // The scan rule retains the full predicate in Calcite. Skip the whole
                    // predicate, including any parameters already collected from its branches.
                    parameters.subList(parameterCount, parameters.size()).clear();
                }
            }
            return expressions.size() == 1
                    ? expressions.get(0)
                    : expressions.stream().map(expression -> "(" + expression + ")")
                            .collect(Collectors.joining(" AND "));
        }

        private String build(RexNode node) {
            if (node instanceof RexInputRef || node instanceof RexLiteral) {
                return operand(node);
            }
            if (!(node instanceof RexCall)) {
                throw new UnsupportedOperationException("Unsupported filter: " + node);
            }
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
                return build(RexUtil.expandSearch(new RexBuilder(new JavaTypeFactoryImpl()), null, call));
            }
            if (call.isA(SqlKind.NOT)) {
                return "NOT (" + build(call.getOperands().get(0)) + ")";
            }
            if (call.getOperands().size() == 1) {
                String operand = operand(call.getOperands().get(0));
                switch (call.getKind()) {
                    case IS_NULL:
                    case IS_UNKNOWN:
                        return operand + " IS NULL";
                    case IS_NOT_NULL:
                        return operand + " IS NOT NULL";
                    case IS_TRUE:
                        return operand + " IS TRUE";
                    case IS_FALSE:
                        return operand + " IS FALSE";
                    case IS_NOT_TRUE:
                        return operand + " IS NOT TRUE";
                    case IS_NOT_FALSE:
                        return operand + " IS NOT FALSE";
                    default:
                        break;
                }
            }
            if (call.getOperands().size() != 2
                    || !(SqlKind.COMPARISON.contains(call.getKind()) || call.isA(SqlKind.LIKE))) {
                throw new UnsupportedOperationException("Unsupported filter: " + call);
            }
            return operand(call.getOperands().get(0)) + " " + operator + " "
                    + operand(call.getOperands().get(1));
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
            throw new UnsupportedOperationException("Unsupported operand kind: " + node.getKind());
        }
    }
}

