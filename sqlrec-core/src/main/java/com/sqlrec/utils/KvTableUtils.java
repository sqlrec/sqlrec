package com.sqlrec.utils;

import com.sqlrec.common.schema.SqlRecTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KvTableUtils {
    public static boolean isRightTableKVTable(RelNode right) {
        return getRightTableKVTable(right) != null;
    }

    public static SqlRecTable getRightTableKVTable(RelNode right) {
        if (right instanceof RelSubset) {
            RelSubset relNode = ((RelSubset) right);
            List<RelNode> inputs = relNode.getRelList();
            for (RelNode input : inputs) {
                if (input instanceof LogicalTableScan) {
                    LogicalTableScan tableScan = (LogicalTableScan) input;
                    SqlRecTable kvTable = getKvTable(tableScan.getTable());
                    if (kvTable != null) {
                        return kvTable;
                    }
                }
            }
        }

        if (right instanceof TableScan) {
            TableScan tableScan = (TableScan) right;
            SqlRecTable kvTable = getKvTable(tableScan.getTable());
            if (kvTable != null) {
                return kvTable;
            }
        }

        return null;
    }

    public static boolean isKvTable(RelOptTable table) {
        return getKvTable(table) != null;
    }

    public static SqlRecTable getKvTable(RelOptTable table) {
        SqlRecTable sqlRecTable = null;
        if (table != null) {
            sqlRecTable = table.unwrap(SqlRecTable.class);
        }
        if (sqlRecTable != null && sqlRecTable.getSqlRecTableType() == SqlRecTable.SqlRecTableType.KV) {
            return sqlRecTable;
        }
        return null;
    }

    public static Map.Entry<Integer, Integer> getJoinKeyColIndex(Join join) {
        RexNode condition = join.getCondition();

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
