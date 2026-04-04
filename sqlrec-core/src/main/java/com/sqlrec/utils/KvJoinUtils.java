package com.sqlrec.utils;

import com.sqlrec.common.schema.SqlRecKvTable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

public class KvJoinUtils {

    public static Enumerable kvJoin(
            Enumerable left,
            SqlRecKvTable rightTable,
            RexNode condition,
            JoinRelType joinType
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

        return joinByPrimaryKey(leftValues, condition, rightTable, joinType);
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
}
