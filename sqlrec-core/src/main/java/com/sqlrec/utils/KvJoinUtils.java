package com.sqlrec.utils;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.utils.MergeUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

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

        Map.Entry<Integer, Integer> joinKeyColIndex = NodeUtils.getJoinKeyColIndex(condition);
        int leftJoinKeyColIndex = joinKeyColIndex.getKey();
        int leftSize = leftValues.get(0).length;
        int rightJoinKeyColIndex = joinKeyColIndex.getValue() - leftSize;
        RelDataType rightRowType = rightTable.getRowType(new JavaTypeFactoryImpl());
        int rightSize = rightRowType.getFieldCount();

        Set<Object> joinKeys = new HashSet<>();
        for (Object[] leftValue : leftValues) {
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            if (leftJoinKey == null) {
                continue;
            }
            joinKeys.add(leftJoinKey);
        }

        Map<Object, List<Object[]>> rightValuesMap;
        if (rightJoinKeyColIndex == rightTable.getPrimaryKeyIndex()) {
            rightValuesMap = rightTable.getByPrimaryKey(joinKeys);
        } else {
            rightValuesMap = scanRightTableByJoinKey(rightTable, rightJoinKeyColIndex, joinKeys);
        }

        Map<String, List<Object[]>> stringKeyMap = new HashMap<>();
        for (Map.Entry<Object, List<Object[]>> entry : rightValuesMap.entrySet()) {
            stringKeyMap.put(entry.getKey().toString(), entry.getValue());
        }

        List<List<Object[]>> rowList = new ArrayList<>();
        for (Object[] leftValue : leftValues) {
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            if (leftJoinKey == null) {
                continue;
            }
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

    private static Map<Object, List<Object[]>> scanRightTableByJoinKey(
            SqlRecKvTable rightTable,
            int rightJoinKeyColIndex,
            Set<Object> joinKeys
    ) {
        if (joinKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        RelDataType rightRowType = rightTable.getRowType(new JavaTypeFactoryImpl());
        RelDataTypeField joinKeyField = rightRowType.getFieldList().get(rightJoinKeyColIndex);
        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());

        Map<Object, List<Object[]>> rightValuesMap = new HashMap<>();
        for (Object key : joinKeys) {
            RexNode inputRef = rexBuilder.makeInputRef(joinKeyField.getType(), rightJoinKeyColIndex);
            RexNode literal = rexBuilder.makeLiteral(key, joinKeyField.getType());
            RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, literal);

            Enumerable<Object[]> result = rightTable.scan(null, Collections.singletonList(filter));
            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : result) {
                rows.add(row);
            }
            if (!rows.isEmpty()) {
                rightValuesMap.put(key, rows);
            }
        }
        return rightValuesMap;
    }

    public static Object[] copyValues(Object[] leftValue, Object[] rightValue, int leftSize, int rightSize) {
        Object[] copy = new Object[leftSize + rightSize];
        System.arraycopy(leftValue, 0, copy, 0, leftSize);
        if (rightValue != null) {
            System.arraycopy(rightValue, 0, copy, leftSize, rightSize);
        }
        return copy;
    }
}
