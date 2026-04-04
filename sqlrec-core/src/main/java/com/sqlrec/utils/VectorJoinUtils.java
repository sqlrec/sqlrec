package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class VectorJoinUtils {

    public static Enumerable vectorJoin(
            Enumerable left,
            SqlRecVectorTable rightTable,
            Object filterConditionObj,
            int leftEmbeddingColIndex,
            String rightEmbeddingColName,
            int limit,
            List<Integer> projectColumns
    ) {
        RexNode filterCondition = (RexNode) filterConditionObj;
        if (left == null) {
            throw new IllegalArgumentException("left table is null");
        }
        List<Object[]> leftValues = new ArrayList<>();
        for (Object obj : left) {
            if (obj instanceof Object[]) {
                leftValues.add((Object[]) obj);
            } else {
                leftValues.add(new Object[]{obj});
            }
        }
        if (leftValues.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }

        int leftSize = leftValues.get(0).length;
        RelDataType rightRowType = rightTable.getRowType(new JavaTypeFactoryImpl());
        List<String> rightFieldNames = rightRowType.getFieldNames();

        return doVectorJoin(
                leftValues,
                rightTable,
                filterCondition,
                leftEmbeddingColIndex,
                rightEmbeddingColName,
                limit,
                projectColumns,
                leftSize,
                rightFieldNames
        );
    }

    private static Enumerable doVectorJoin(
            List<Object[]> leftValues,
            SqlRecVectorTable rightTable,
            RexNode filterCondition,
            int leftEmbeddingColIndex,
            String rightEmbeddingColName,
            int limit,
            List<Integer> projectColumns,
            int leftSize,
            List<String> rightFieldNames
    ) {
        int rightSize = rightFieldNames.size() + 1;  // 右表模式多一个相似度分数列
        int projectSize = projectColumns != null ? projectColumns.size() : (leftSize + rightSize);

        if (limit <= 0) {
            limit = SqlRecConfigs.DEFAULT_VECTOR_SEARCH_LIMIT.getValue();
        }

        List<Integer> vectorProjectColumns = new ArrayList<>();
        if (projectColumns == null || projectColumns.isEmpty()) {
            for (int i = 0; i < rightSize; i++) {
                vectorProjectColumns.add(i);
            }
        } else {
            for (int index : projectColumns) {
                if (index >= leftSize) {
                    vectorProjectColumns.add(index - leftSize);
                }
            }
        }

        List<Object[]> result = new ArrayList<>();
        for (Object[] leftValue : leftValues) {
            Object leftEmbedding = leftValue[leftEmbeddingColIndex];
            List<Float> embedding = DataTransformUtils.convertToFloatVec(leftEmbedding);

            String filterExpression = buildFilterExpression(
                    filterCondition,
                    leftValue,
                    leftSize,
                    rightFieldNames
            );

            List<Object[]> rightValues = rightTable.searchByEmbeddingWithScore(
                    rightEmbeddingColName,
                    embedding,
                    filterExpression,
                    limit,
                    vectorProjectColumns
            );

            if (rightValues == null || rightValues.isEmpty()) {
                // 对于INNER JOIN，不添加结果；对于LEFT JOIN，添加null行
                // 这里暂时按INNER JOIN处理，如果需要LEFT JOIN可以调整
            } else {
                for (Object[] rightValue : rightValues) {
                    Object[] joinRow = copyValues(leftValue, rightValue, leftSize, rightSize);
                    Object[] projectRow = buildProjectRow(joinRow, projectColumns, projectSize);
                    result.add(projectRow);
                }
            }
        }
        return Linq4j.asEnumerable(result);
    }

    private static Object[] buildProjectRow(Object[] joinRow, List<Integer> projectColumns, int projectSize) {
        if (projectColumns == null || projectColumns.isEmpty()) {
            return joinRow;
        }
        Object[] projectRow = new Object[projectSize];
        for (int i = 0; i < projectColumns.size() && i < projectSize; i++) {
            Integer colIndex = projectColumns.get(i);
            if (colIndex != null && colIndex < joinRow.length) {
                projectRow[i] = joinRow[colIndex];
            }
        }
        return projectRow;
    }

    private static Object[] copyValues(Object[] leftValue, Object[] rightValue, int leftSize, int rightSize) {
        Object[] copy = new Object[leftSize + rightSize];
        System.arraycopy(leftValue, 0, copy, 0, leftSize);
        if (rightValue != null) {
            System.arraycopy(rightValue, 0, copy, leftSize, rightSize);
        }
        return copy;
    }

    public static String buildFilterExpression(
            RexNode filterCondition,
            Object[] leftValue,
            int leftSize,
            List<String> rightFieldNames) {

        if (filterCondition == null) {
            return null;
        }

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
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return value.toString();
    }

    private static String getOperator(String opName) {
        switch (opName.toUpperCase()) {
            case "=":
                return "==";
            case ">":
                return ">";
            case "<":
                return "<";
            case ">=":
                return ">=";
            case "<=":
                return "<=";
            case "!=":
                return "!=";
            default:
                return opName;
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

    public static VectorJoinConfig extractVectorJoinConfig(
            LogicalSort sort,
            LogicalProject project,
            LogicalFilter filter,
            LogicalJoin join) {

        VectorJoinConfig config = new VectorJoinConfig();

        if (sort != null && sort.fetch != null && sort.fetch instanceof RexLiteral) {
            config.limit = ((RexLiteral) sort.fetch).getValueAs(Integer.class);
        }

        RelDataType leftRowType = join.getLeft().getRowType();
        RelDataType rightRowType = join.getRight().getRowType();
        int leftSize = leftRowType.getFieldCount();
        List<String> rightFieldNames = rightRowType.getFieldNames();

        for (RexNode node : project.getProjects()) {
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.getOperator().getName().equalsIgnoreCase("ip")) {
                    RexNode operand0 = call.getOperands().get(0);
                    RexNode operand1 = call.getOperands().get(1);

                    if (operand0 instanceof RexInputRef && operand1 instanceof RexInputRef) {
                        int leftIdx = ((RexInputRef) operand0).getIndex();
                        int rightIdx = ((RexInputRef) operand1).getIndex();

                        if (leftIdx < leftSize && rightIdx >= leftSize) {
                            config.leftEmbeddingColIndex = leftIdx;
                            config.rightEmbeddingColIndex = rightIdx - leftSize;
                        } else if (rightIdx < leftSize && leftIdx >= leftSize) {
                            config.leftEmbeddingColIndex = rightIdx;
                            config.rightEmbeddingColIndex = leftIdx - leftSize;
                        }

                        config.rightEmbeddingColName = rightFieldNames.get(config.rightEmbeddingColIndex);
                    }
                    break;
                }
            }
        }

        if (filter != null) {
            config.filterCondition = filter.getCondition();
        }

        List<Integer> projectColumns = new ArrayList<>();
        for (RexNode node : project.getProjects()) {
            if (node instanceof RexInputRef) {
                projectColumns.add(((RexInputRef) node).getIndex());
            }
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.getOperator().getName().equalsIgnoreCase("ip")) {
                    projectColumns.add(leftSize + rightFieldNames.size());
                }
            }
        }
        config.projectColumns = projectColumns;

        config.projectRowType = project.getRowType();
        if (sort != null) {
            config.collation = sort.collation;
        }

        return config;
    }

    public static boolean hasIpFunction(LogicalProject project) {
        for (RexNode node : project.getProjects()) {
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.getOperator().getName().equalsIgnoreCase("ip")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isTrueCondition(LogicalJoin join) {
        RexNode condition = join.getCondition();
        if (condition instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) condition;
            if (literal.getValue() instanceof Boolean) {
                return (Boolean) literal.getValue();
            }
            if (literal.getValue() instanceof BigDecimal) {
                return ((BigDecimal) literal.getValue()).compareTo(BigDecimal.ONE) == 0;
            }
        }
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                RexNode left = call.getOperands().get(0);
                RexNode right = call.getOperands().get(1);
                if (left instanceof RexLiteral && right instanceof RexLiteral) {
                    return left.equals(right);
                }
            }
        }
        return false;
    }

    public static class VectorJoinConfig {
        public int leftEmbeddingColIndex;
        public int rightEmbeddingColIndex;
        public String rightEmbeddingColName;
        public RexNode filterCondition;
        public int limit;
        public List<Integer> projectColumns = new ArrayList<>();
        public RelDataType projectRowType;
        public RelCollation collation;
    }
}
