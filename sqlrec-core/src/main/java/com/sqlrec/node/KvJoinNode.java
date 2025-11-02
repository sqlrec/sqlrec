package com.sqlrec.node;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.JoinUtils;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.interpreter.*;
import org.apache.calcite.interpreter.Compiler;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.*;

public class KvJoinNode implements Node {
    private final Source leftSource;
    private final Sink sink;
    private final Join rel;
    private final int leftSize;
    private final int rightSize;

    public KvJoinNode(Compiler compiler, Join rel) {
        this.leftSource = compiler.source(rel, 0);
        this.sink = compiler.sink(rel);
        this.rel = rel;
        this.leftSize = rel.getLeft().getRowType().getFieldCount();
        this.rightSize = rel.getRight().getRowType().getFieldCount();
    }

    @Override
    public void run() throws InterruptedException {
        SqlRecKvTable rightTable = KvTableUtils.getRightTableKVTable(rel.getRight());
        if (rightTable == null) {
            throw new IllegalArgumentException("right table is not kv table");
        }

        Map.Entry<Integer, Integer> joinIpColIndex = KvTableUtils.getJoinIpColIndex(rel);
        if (joinIpColIndex != null) {
            if (rightTable instanceof SqlRecVectorTable) {
                joinBySearch(joinIpColIndex, (SqlRecVectorTable) rightTable);
            } else {
                throw new IllegalArgumentException("right table is not vector table");
            }
        } else {
            joinByPrimaryKey(rightTable);
        }
    }

    private void joinBySearch(Map.Entry<Integer, Integer> joinIpColIndex, SqlRecVectorTable rightTable) throws InterruptedException {
        int limit = 0;
        List<Integer> projectColumns = null;

        if (rel instanceof SqlRecJoin) {
            SqlRecJoin sqlRecJoin = (SqlRecJoin) rel;
            limit = sqlRecJoin.getLimit();
            projectColumns = new ArrayList<>();
            for (int index : sqlRecJoin.getProjectList()) {
                if (index >= leftSize) {
                    projectColumns.add(index - leftSize);
                }
            }
        }
        if (projectColumns == null || projectColumns.isEmpty()) {
            projectColumns = new ArrayList<>();
            for (int i = 0; i < rightSize; i++) {
                projectColumns.add(i);
            }
        }

        int leftJoinKeyColIndex = joinIpColIndex.getKey();
        int rightJoinKeyColIndex = joinIpColIndex.getValue() - leftSize;
        String rightJoinKeyColName = rel.getRight().getRowType().getFieldNames().get(rightJoinKeyColIndex);

        Row leftRow = null;
        while ((leftRow = leftSource.receive()) != null) {
            Object[] leftValue = leftRow.copyValues();
            Object leftJoinIp = leftValue[leftJoinKeyColIndex];
            List<Float> embedding = JoinUtils.convertToFloat(leftJoinIp);
            List<Object[]> rightValues = rightTable.searchByEmbedding(rightJoinKeyColName, embedding, limit, projectColumns);
            if (rightValues == null || rightValues.isEmpty()) {
                if (rel.getJoinType() == JoinRelType.LEFT) {
                    send(leftValue, null);
                }
            } else {
                for (Object[] rightValue : rightValues) {
                    send(leftValue, rightValue);
                }
            }
        }
    }

    private void joinByPrimaryKey(SqlRecKvTable rightTable) throws InterruptedException {
        Map.Entry<Integer, Integer> joinKeyColIndex = KvTableUtils.getJoinKeyColIndex(rel);
        int leftJoinKeyColIndex = joinKeyColIndex.getKey();

        Row leftRow = null;
        Set<Object> joinKeys = new HashSet<>();
        List<Object[]> leftValues = new ArrayList<>();
        while ((leftRow = leftSource.receive()) != null) {
            Object[] leftValue = leftRow.copyValues();
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            joinKeys.add(leftJoinKey);
            leftValues.add(leftValue);
        }

        Map<Object, List<Object[]>> rightValuesMap = rightTable.getByPrimaryKey(joinKeys);
        for (Object[] leftValue : leftValues) {
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            List<Object[]> rightValues = rightValuesMap.getOrDefault(leftJoinKey, new ArrayList<>());
            if (rightValues.isEmpty()) {
                if (rel.getJoinType() == JoinRelType.LEFT) {
                    send(leftValue, null);
                }
            } else {
                for (Object[] rightValue : rightValues) {
                    send(leftValue, rightValue);
                }
            }
        }
    }

    private void send(Object[] leftValue, Object[] rightValue) throws InterruptedException {
        Object[] row = new Object[leftSize + rightSize];
        System.arraycopy(leftValue, 0, row, 0, leftSize);
        if (rightValue != null) {
            System.arraycopy(rightValue, 0, row, leftSize, rightSize);
        }
        sink.send(Row.of(row));
    }

    @Override
    public void close() {
        leftSource.close();
    }
}
