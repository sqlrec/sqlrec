package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.*;

public class JoinUtils {
    public static Enumerable join(
            Enumerable left,
            SqlRecTable rightTable,
            RexNode condition,
            JoinRelType joinType,
            int limit,
            List<Integer> projectList
    ) {
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

        Map.Entry<Integer, Integer> joinIpColIndex = getJoinIpColIndex(condition);
        if (joinIpColIndex != null) {
            if (rightTable instanceof SqlRecVectorTable) {
                return joinBySearch(
                        leftValues,
                        joinIpColIndex,
                        (SqlRecVectorTable) rightTable,
                        joinType,
                        limit,
                        projectList
                );
            } else {
                throw new IllegalArgumentException("right table is not vector table");
            }
        } else {
            if (rightTable instanceof SqlRecKvTable) {
                return joinByPrimaryKey(
                        leftValues,
                        condition,
                        (SqlRecKvTable) rightTable,
                        joinType
                );
            } else {
                throw new IllegalArgumentException("right table is not kv table");
            }
        }
    }

    private static Enumerable joinBySearch(
            List<Object[]> leftValues,
            Map.Entry<Integer, Integer> joinIpColIndex,
            SqlRecVectorTable rightTable,
            JoinRelType joinType,
            int limit,
            List<Integer> projectColumns
    ) {
        RelDataType rightRowType = rightTable.getRowType(new JavaTypeFactoryImpl());
        int rightSize = rightRowType.getFieldCount();
        int leftSize = leftValues.get(0).length;

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

        int leftJoinKeyColIndex = joinIpColIndex.getKey();
        int rightJoinKeyColIndex = joinIpColIndex.getValue() - leftSize;
        String rightJoinKeyColName = rightRowType.getFieldNames().get(rightJoinKeyColIndex);

        List<Object[]> result = new ArrayList<>();
        for (Object[] leftValue : leftValues) {
            Object leftJoinIp = leftValue[leftJoinKeyColIndex];
            List<Float> embedding = DataTransformUtils.convertToFloatVec(leftJoinIp);
            List<Object[]> rightValues = rightTable.searchByEmbedding(
                    rightJoinKeyColName, embedding, limit, vectorProjectColumns
            );
            if (rightValues == null || rightValues.isEmpty()) {
                if (joinType == JoinRelType.LEFT) {
                    result.add(copyValues(leftValue, null, leftSize, rightSize));
                }
            } else {
                for (Object[] rightValue : rightValues) {
                    result.add(copyValues(leftValue, rightValue, leftSize, rightSize));
                }
            }
        }
        return Linq4j.asEnumerable(result);
    }

    private static Enumerable joinByPrimaryKey(
            List<Object[]> leftValues,
            RexNode condition,
            SqlRecKvTable rightTable,
            JoinRelType joinType
    ) {
        Map.Entry<Integer, Integer> joinKeyColIndex = getJoinKeyColIndex(condition);
        int leftJoinKeyColIndex = joinKeyColIndex.getKey();
        int leftSize = leftValues.get(0).length;
        RelDataType rightRowType = rightTable.getRowType(new JavaTypeFactoryImpl());
        int rightSize = rightRowType.getFieldCount();

        Set<Object> joinKeys = new HashSet<>();
        for (Object[] leftValue : leftValues) {
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            joinKeys.add(leftJoinKey);
        }

        Map<Object, List<Object[]>> rightValuesMap = rightTable.getByPrimaryKeyWithCache(joinKeys);
        Map<String, List<Object[]>> stringKeyMap = new HashMap<>();
        for (Map.Entry<Object, List<Object[]>> entry : rightValuesMap.entrySet()) {
            stringKeyMap.put(entry.getKey().toString(), entry.getValue());
        }

        List<List<Object[]>> rowList = new ArrayList<>();
        for (Object[] leftValue : leftValues) {
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            List<Object[]> rightValues = stringKeyMap.getOrDefault(leftJoinKey.toString(), new ArrayList<>());
            if (rightValues.isEmpty()) {
                if (joinType == JoinRelType.LEFT) {
                    rowList.add(Collections.singletonList(copyValues(leftValue, null, leftSize, rightSize)));
                }
            } else {
                List<Object[]> rightRows = new ArrayList<>(rightValues.size());
                for (Object[] rightValue : rightValues) {
                    rightRows.add(copyValues(leftValue, rightValue, leftSize, rightSize));
                }
                rowList.add(rightRows);
            }
        }

        List<Object[]> merged = MergeUtils.snakeMerge(rowList.toArray(new Iterable[0]));
        return Linq4j.asEnumerable(merged);
    }

    private static Object[] copyValues(Object[] leftValue, Object[] rightValue, int leftSize, int rightSize) {
        Object[] copy = new Object[leftSize + rightSize];
        System.arraycopy(leftValue, 0, copy, 0, leftSize);
        if (rightValue != null) {
            System.arraycopy(rightValue, 0, copy, leftSize, rightSize);
        }
        return copy;
    }


    public static Map.Entry<Integer, Integer> getJoinKeyColIndex(Join join) {
        RexNode condition = join.getCondition();
        return getJoinKeyColIndex(condition);
    }

    public static Map.Entry<Integer, Integer> getJoinKeyColIndex(RexNode condition) {
        List<Integer> indexList = new ArrayList<>();
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexInputRef) {
                        indexList.add(((RexInputRef) operand).getIndex());
                    } else {
                        throw new UnsupportedOperationException("Join condition operand must be RexInputRef");
                    }
                }
            } else {
                throw new UnsupportedOperationException("Join condition must be RexCall of kind EQUALS");
            }
        } else {
            throw new UnsupportedOperationException("Join condition must be RexCall");
        }

        if (indexList.size() != 2) {
            throw new UnsupportedOperationException("Join condition must be RexCall of kind EQUALS with 2 operands");
        }

        indexList.sort(Integer::compareTo);
        return Map.entry(indexList.get(0), indexList.get(1));
    }

    public static Map.Entry<Integer, Integer> getJoinIpColIndex(Join join) {
        RexNode condition = join.getCondition();
        return getJoinIpColIndex(condition);
    }

    public static Map.Entry<Integer, Integer> getJoinIpColIndex(RexNode condition) {
        List<Integer> indexList = new ArrayList<>();
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            for (RexNode operand : call.getOperands()) {
                if (operand instanceof RexCall) {
                    RexCall ipCall = (RexCall) operand;
                    if (ipCall.getOperator().getName().equalsIgnoreCase("ip")) {
                        indexList.add(((RexInputRef) ipCall.getOperands().get(0)).getIndex());
                        indexList.add(((RexInputRef) ipCall.getOperands().get(1)).getIndex());
                        break;
                    }
                }
            }
        } else {
            return null;
        }

        if (indexList.size() != 2) {
            return null;
        }

        indexList.sort(Integer::compareTo);
        return Map.entry(indexList.get(0), indexList.get(1));
    }

    public static JoinPostProcessConfig findJoinPostProcessConfig(RelNode root, Join join) {
        List<Join> joinNodes = new ArrayList<>();
        findJoinNodes(root, join, joinNodes);
        if (joinNodes.size() != 1) {
            return null;
        }

        JoinPostProcessConfig joinPostProcessConfig = new JoinPostProcessConfig();
        findJoinPostProcessConfig(root, join, joinPostProcessConfig);
        if (!joinPostProcessConfig.hasFindJoin) {
            return null;
        }
        return joinPostProcessConfig;
    }

    public static void findJoinNodes(RelNode root, Join join, List<Join> joinNodes) {
        if (root instanceof Join) {
            joinNodes.add((Join) root);
        }

        List<RelNode> inputs = root.getInputs();
        for (RelNode input : inputs) {
            findJoinNodes(input, join, joinNodes);
        }
    }

    public static void findJoinPostProcessConfig(RelNode root, Join join, JoinPostProcessConfig joinPostProcessConfig) {
        if (root instanceof Join) {
            joinPostProcessConfig.hasFindJoin = true;
            return;
        }

        int oldLimit = joinPostProcessConfig.limit;
        List<Integer> oldProjectColumns = joinPostProcessConfig.projectColumns;

        if (root instanceof LogicalSort) {
            LogicalSort logicalSort = (LogicalSort) root;
            if (logicalSort.fetch != null && logicalSort.fetch instanceof RexLiteral) {
                int limit = ((RexLiteral) logicalSort.fetch).getValueAs(BigDecimal.class).intValue();
                joinPostProcessConfig.limit = limit;
            }
        } else if (root instanceof LogicalProject) {
            LogicalProject logicalProject = (LogicalProject) root;
            List<Integer> projectColumns = new ArrayList<>();
            for (RexNode node : logicalProject.getProjects()) {
                if (node instanceof RexInputRef) {
                    projectColumns.add(((RexInputRef) node).getIndex());
                }
            }
            joinPostProcessConfig.projectColumns = projectColumns;
        } else {
            joinPostProcessConfig.limit = 0;
            joinPostProcessConfig.projectColumns = new ArrayList<>();
        }

        List<RelNode> inputs = root.getInputs();
        for (RelNode input : inputs) {
            findJoinPostProcessConfig(input, join, joinPostProcessConfig);
            if (joinPostProcessConfig.hasFindJoin) {
                return;
            }
        }

        joinPostProcessConfig.limit = oldLimit;
        joinPostProcessConfig.projectColumns = oldProjectColumns;
    }

    public static class JoinPostProcessConfig {
        public boolean hasFindJoin = false;
        public int limit = 0;
        public List<Integer> projectColumns = new ArrayList<>();
    }
}
