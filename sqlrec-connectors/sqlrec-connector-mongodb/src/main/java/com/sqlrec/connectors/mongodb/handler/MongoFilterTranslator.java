package com.sqlrec.connectors.mongodb.handler;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Filters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Translates Calcite predicates into MongoDB filters without performing I/O. */
final class MongoFilterTranslator {
    private static final Logger logger = LoggerFactory.getLogger(MongoFilterTranslator.class);
    private final MongoConfig mongoConfig;

    MongoFilterTranslator(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
    }

    Bson buildQuery(List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            return new Document();
        }
        List<Bson> bsonFilters = filters.stream()
                .map(filter -> buildBsonFilter(filter))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (bsonFilters.isEmpty()) {
            return new Document();
        }
        if (bsonFilters.size() == 1) {
            return bsonFilters.get(0);
        }
        return Filters.and(bsonFilters);
    }

    private static final Map<SqlKind, java.util.function.BiFunction<String, Object, Bson>> COMPARISON_OPS = new EnumMap<>(SqlKind.class);
    private static final Map<SqlKind, SqlKind> REVERSED_COMPARISON = new EnumMap<>(SqlKind.class);

    static {
        COMPARISON_OPS.put(SqlKind.EQUALS, Filters::eq);
        COMPARISON_OPS.put(SqlKind.NOT_EQUALS, Filters::ne);
        COMPARISON_OPS.put(SqlKind.GREATER_THAN, Filters::gt);
        COMPARISON_OPS.put(SqlKind.GREATER_THAN_OR_EQUAL, Filters::gte);
        COMPARISON_OPS.put(SqlKind.LESS_THAN, Filters::lt);
        COMPARISON_OPS.put(SqlKind.LESS_THAN_OR_EQUAL, Filters::lte);

        REVERSED_COMPARISON.put(SqlKind.GREATER_THAN, SqlKind.LESS_THAN);
        REVERSED_COMPARISON.put(SqlKind.LESS_THAN, SqlKind.GREATER_THAN);
        REVERSED_COMPARISON.put(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);
        REVERSED_COMPARISON.put(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL);
    }

    private Bson buildBsonFilter(RexNode filter) {
        if (filter instanceof RexInputRef) {
            return Filters.eq(getFieldName(filter), true);
        }
        if (filter instanceof RexLiteral && filter.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            Boolean value = ((RexLiteral) filter).getValueAs(Boolean.class);
            return Boolean.TRUE.equals(value) ? new Document() : Filters.expr(false);
        }
        if (filter.isA(SqlKind.AND) || filter.isA(SqlKind.OR)) {
            RexCall call = (RexCall) filter;
            boolean isOr = filter.isA(SqlKind.OR);
            List<Bson> operands = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                Bson bsonOperand = buildBsonFilter(operand);
                if (bsonOperand == null) {
                    // Dropping an unsupported AND operand only broadens the MongoDB result set,
                    // so Calcite can still apply the complete residual filter safely. Dropping an
                    // OR operand narrows the result set and would permanently lose matching rows.
                    if (isOr) {
                        return null;
                    }
                    continue;
                }
                operands.add(bsonOperand);
            }
            if (operands.isEmpty()) {
                return null;
            }
            return isOr ? Filters.or(operands) : Filters.and(operands);
        }
        if (filter.isA(SqlKind.NOT)) {
            return buildNotFilter((RexCall) filter);
        }
        if (filter.isA(SqlKind.LIKE)) {
            return buildLikeFilter((RexCall) filter);
        }
        if (filter.isA(SqlKind.SEARCH)) {
            return buildSearchFilter((RexCall) filter);
        }
        if (filter.isA(SqlKind.IS_NULL) || filter.isA(SqlKind.IS_NOT_NULL)
                || filter.isA(SqlKind.IS_UNKNOWN)) {
            RexCall call = (RexCall) filter;
            String fieldName = getFieldName(call.getOperands().get(0));
            if (fieldName == null) {
                return null;
            }
            return filter.isA(SqlKind.IS_NOT_NULL) ? Filters.ne(fieldName, null) : Filters.eq(fieldName, null);
        }
        if (filter.isA(SqlKind.IS_TRUE) || filter.isA(SqlKind.IS_FALSE)
                || filter.isA(SqlKind.IS_NOT_TRUE) || filter.isA(SqlKind.IS_NOT_FALSE)) {
            return buildBooleanFilter((RexCall) filter);
        }
        if (filter.isA(SqlKind.IS_DISTINCT_FROM) || filter.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
            RexCall call = (RexCall) filter;
            return buildComparisonFilter(call,
                    filter.isA(SqlKind.IS_DISTINCT_FROM) ? Filters::ne : Filters::eq);
        }
        java.util.function.BiFunction<String, Object, Bson> op = COMPARISON_OPS.get(filter.getKind());
        if (op != null) {
            return buildComparisonFilter((RexCall) filter, op);
        }
        logger.warn("Unsupported filter kind: {}, filter will be handled by Calcite", filter.getKind());
        return null;
    }

    private Bson buildNotFilter(RexCall call) {
        RexNode operand = call.getOperands().get(0);
        if (operand instanceof RexInputRef) {
            return Filters.eq(getFieldName(operand), false);
        }
        // AND may have been weakened by dropping unsupported operands. Negating that broader
        // predicate could narrow the result set, so composite negation stays with Calcite.
        if (operand.isA(SqlKind.AND) || operand.isA(SqlKind.OR) || operand.isA(SqlKind.NOT)) {
            return null;
        }
        Bson bsonOperand = buildBsonFilter(operand);
        return bsonOperand == null ? null : Filters.nor(bsonOperand);
    }

    private Bson buildLikeFilter(RexCall call) {
        String fieldName = getFieldName(call.getOperands().get(0));
        RexLiteral patternLiteral = getLiteral(call.getOperands().get(1));
        if (fieldName == null || patternLiteral == null) {
            return null;
        }

        String pattern = patternLiteral.getValueAs(String.class);
        Character escape = null;
        if (call.getOperands().size() == 3) {
            RexLiteral escapeLiteral = getLiteral(call.getOperands().get(2));
            String escapeString = escapeLiteral == null ? null : escapeLiteral.getValueAs(String.class);
            if (escapeString == null || escapeString.length() != 1) {
                return null;
            }
            escape = escapeString.charAt(0);
        }

        String regex = convertLikePattern(pattern, escape);
        if (regex == null) {
            return null;
        }
        SqlLikeOperator operator = (SqlLikeOperator) call.getOperator();
        // SQL wildcards also match line terminators. MongoDB's regex dot does not unless
        // dotAll is enabled, which would otherwise make LIKE pushdown lose valid rows.
        String regexOptions = operator.isCaseSensitive() ? "s" : "is";
        Bson likeFilter = Filters.regex(fieldName, regex, regexOptions);
        return operator.isNegated() ? Filters.nor(likeFilter) : likeFilter;
    }

    private String convertLikePattern(String pattern, Character escape) {
        if (pattern == null) {
            return null;
        }
        StringBuilder regex = new StringBuilder("^");
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (!escaped && escape != null && c == escape) {
                escaped = true;
                continue;
            }
            if (!escaped && c == '%') {
                regex.append(".*");
            } else if (!escaped && c == '_') {
                regex.append('.');
            } else {
                if ("\\.^$|?*+()[]{}".indexOf(c) >= 0) {
                    regex.append('\\');
                }
                regex.append(c);
            }
            escaped = false;
        }
        if (escaped) {
            return null;
        }
        return regex.append('$').toString();
    }

    private Bson buildBooleanFilter(RexCall call) {
        String fieldName = getFieldName(call.getOperands().get(0));
        if (fieldName == null) {
            return null;
        }
        if (call.isA(SqlKind.IS_TRUE)) {
            return Filters.eq(fieldName, true);
        }
        if (call.isA(SqlKind.IS_FALSE)) {
            return Filters.eq(fieldName, false);
        }
        return call.isA(SqlKind.IS_NOT_TRUE)
                ? Filters.ne(fieldName, true)
                : Filters.ne(fieldName, false);
    }

    private Bson buildSearchFilter(RexCall call) {
        if (call.getOperands().size() != 2) {
            return null;
        }
        RexNode fieldNode = call.getOperands().get(0);
        String fieldName = getFieldName(fieldNode);
        RexLiteral sargLiteral = getLiteral(call.getOperands().get(1));
        Sarg<?> sarg = sargLiteral == null ? null : sargLiteral.getValueAs(Sarg.class);
        if (fieldName == null || sarg == null) {
            return null;
        }

        Bson pointFilter = buildPointSearchFilter(fieldName, fieldNode.getType().getSqlTypeName(), sarg);
        if (pointFilter != null) {
            if (sarg.nullAs == RexUnknownAs.TRUE) {
                return Filters.or(pointFilter, Filters.eq(fieldName, null));
            }
            return pointFilter;
        }

        List<Bson> alternatives = new ArrayList<>();
        for (Range<?> range : sarg.rangeSet.asRanges()) {
            Bson rangeFilter = buildRangeFilter(fieldName, fieldNode.getType().getSqlTypeName(), range);
            if (rangeFilter == null) {
                return null;
            }
            alternatives.add(rangeFilter);
        }
        if (sarg.nullAs == RexUnknownAs.TRUE) {
            alternatives.add(Filters.eq(fieldName, null));
        }
        if (alternatives.isEmpty()) {
            return Filters.expr(false);
        }
        return alternatives.size() == 1 ? alternatives.get(0) : Filters.or(alternatives);
    }

    private Bson buildPointSearchFilter(String fieldName, SqlTypeName typeName, Sarg<?> sarg) {
        boolean complemented = sarg.isComplementedPoints();
        if (!sarg.isPoints() && !complemented) {
            return null;
        }
        Set<? extends Range<?>> ranges = complemented
                ? sarg.rangeSet.complement().asRanges()
                : sarg.rangeSet.asRanges();
        List<Object> values = new ArrayList<>(ranges.size());
        for (Range<?> range : ranges) {
            if (!range.hasLowerBound() || !range.hasUpperBound()
                    || range.lowerBoundType() != BoundType.CLOSED
                    || range.upperBoundType() != BoundType.CLOSED
                    || !range.lowerEndpoint().equals(range.upperEndpoint())) {
                return null;
            }
            values.add(convertSargValue(range.lowerEndpoint(), typeName));
        }
        return complemented ? Filters.nin(fieldName, values) : Filters.in(fieldName, values);
    }

    private Bson buildRangeFilter(String fieldName, SqlTypeName typeName, Range<?> range) {
        if (!range.hasLowerBound() && !range.hasUpperBound()) {
            return new Document();
        }
        if (range.hasLowerBound() && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint().equals(range.upperEndpoint())) {
            return Filters.eq(fieldName, convertSargValue(range.lowerEndpoint(), typeName));
        }

        List<Bson> bounds = new ArrayList<>(2);
        if (range.hasLowerBound()) {
            Object value = convertSargValue(range.lowerEndpoint(), typeName);
            bounds.add(range.lowerBoundType() == BoundType.CLOSED
                    ? Filters.gte(fieldName, value)
                    : Filters.gt(fieldName, value));
        }
        if (range.hasUpperBound()) {
            Object value = convertSargValue(range.upperEndpoint(), typeName);
            bounds.add(range.upperBoundType() == BoundType.CLOSED
                    ? Filters.lte(fieldName, value)
                    : Filters.lt(fieldName, value));
        }
        return bounds.size() == 1 ? bounds.get(0) : Filters.and(bounds);
    }

    private Object convertSargValue(Object value, SqlTypeName typeName) {
        if (value instanceof NlsString) {
            return ((NlsString) value).getValue();
        }
        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            switch (typeName) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    return decimal.intValueExact();
                case BIGINT:
                    return decimal.longValueExact();
                case FLOAT:
                case REAL:
                case DOUBLE:
                    return decimal.doubleValue();
                default:
                    return decimal;
            }
        }
        return value;
    }

    private Bson buildComparisonFilter(RexCall call, java.util.function.BiFunction<String, Object, Bson> filterFactory) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        String fieldName = getFieldName(left);
        RexLiteral literal = getLiteral(right);
        if (fieldName != null && literal != null) {
            return filterFactory.apply(fieldName, literal.getValue2());
        }

        // Try reversed: literal on left, field on right
        fieldName = getFieldName(right);
        literal = getLiteral(left);
        if (fieldName != null && literal != null) {
            SqlKind reversed = REVERSED_COMPARISON.getOrDefault(call.getKind(), call.getKind());
            java.util.function.BiFunction<String, Object, Bson> reversedOp = COMPARISON_OPS.get(reversed);
            return (reversedOp != null ? reversedOp : filterFactory).apply(fieldName, literal.getValue2());
        }

        String leftFieldName = getFieldName(left);
        String rightFieldName = getFieldName(right);
        if (leftFieldName != null && rightFieldName != null) {
            String mongoOperator = getMongoExpressionComparison(call.getKind());
            if (mongoOperator != null) {
                return Filters.expr(new Document(mongoOperator,
                        Arrays.asList("$" + leftFieldName, "$" + rightFieldName)));
            }
        }
        logger.warn("Cannot convert filter to MongoDB query: {}", call);
        return null;
    }

    private String getMongoExpressionComparison(SqlKind kind) {
        switch (kind) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                return "$eq";
            case NOT_EQUALS:
            case IS_DISTINCT_FROM:
                return "$ne";
            case GREATER_THAN:
                return "$gt";
            case GREATER_THAN_OR_EQUAL:
                return "$gte";
            case LESS_THAN:
                return "$lt";
            case LESS_THAN_OR_EQUAL:
                return "$lte";
            default:
                return null;
        }
    }

    private String getFieldName(RexNode node) {
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return mongoConfig.fieldSchemas.get(index).getName();
        }
        return null;
    }

    private RexLiteral getLiteral(RexNode node) {
        return node instanceof RexLiteral ? (RexLiteral) node : null;
    }
}
