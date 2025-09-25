package com.sqlrec.node;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.interpreter.*;
import org.apache.calcite.interpreter.Compiler;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;
import java.util.Map;

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

        Map.Entry<Integer, Integer> joinKeyColIndex = KvTableUtils.getJoinKeyColIndex(rel);
        int leftJoinKeyColIndex = joinKeyColIndex.getKey();

        Row leftRow = null;
        while ((leftRow = leftSource.receive()) != null) {
            Object[] leftValue = leftRow.copyValues();
            Object leftJoinKey = leftValue[leftJoinKeyColIndex];
            List<Object[]> rightValues = rightTable.getByPrimaryKey(leftJoinKey);
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
