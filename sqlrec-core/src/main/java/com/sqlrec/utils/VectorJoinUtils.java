package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
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

        return doVectorJoin(
                leftValues,
                rightTable,
                filterCondition,
                leftEmbeddingColIndex,
                rightEmbeddingColName,
                limit,
                projectColumns
        );
    }

    private static Enumerable doVectorJoin(
            List<Object[]> leftValues,
            SqlRecVectorTable rightTable,
            RexNode filterCondition,
            int leftEmbeddingColIndex,
            String rightEmbeddingColName,
            int limit,
            List<Integer> projectColumns
    ) {
        int leftSize = leftValues.get(0).length;
        List<String> rightFieldNames = DataTypeUtils.getTableFieldNames(rightTable);
        int rightSize = rightFieldNames.size() + 1;
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
            if (leftValue == null) {
                continue;
            }
            Object leftEmbedding = leftEmbeddingColIndex >= 0 && leftEmbeddingColIndex < leftValue.length ? 
                leftValue[leftEmbeddingColIndex] : null;
            if (leftEmbedding == null) {
                continue;
            }
            List<Float> embedding = DataTransformUtils.convertToFloatVec(leftEmbedding);
            List<Object[]> rightValues = rightTable.searchByEmbeddingWithScore(
                    leftValue,
                    embedding,
                    rightEmbeddingColName,
                    filterCondition,
                    limit,
                    vectorProjectColumns
            );
            if (rightValues != null) {
                for (Object[] rightValue : rightValues) {
                    Object[] joinRow = KvJoinUtils.copyValues(leftValue, rightValue, leftSize, rightSize);
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

    public static VectorJoinConfig extractVectorJoinConfig(
            LogicalSort sort,
            LogicalProject project,
            LogicalFilter filter,
            LogicalJoin join
    ) {
        VectorJoinConfig config = new VectorJoinConfig();

        if (sort != null && sort.fetch != null && sort.fetch instanceof RexLiteral) {
            Integer limitValue = ((RexLiteral) sort.fetch).getValueAs(Integer.class);
            config.limit = limitValue != null ? limitValue : 0;
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
