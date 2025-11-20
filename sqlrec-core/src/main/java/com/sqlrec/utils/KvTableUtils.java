package com.sqlrec.utils;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KvTableUtils {
    public static boolean isRightTableKVTable(RelNode right) {
        return getRightTableKVTable(right) != null;
    }

    public static SqlRecKvTable getRightTableKVTable(RelNode right) {
        if (right instanceof RelSubset) {
            RelSubset relNode = ((RelSubset) right);
            List<RelNode> inputs = relNode.getRelList();
            for (RelNode input : inputs) {
                if (input instanceof TableScan) {
                    TableScan tableScan = (TableScan) input;
                    SqlRecKvTable kvTable = getKvTable(tableScan.getTable());
                    if (kvTable != null) {
                        return kvTable;
                    }
                }
            }
        }

        if (right instanceof TableScan) {
            TableScan tableScan = (TableScan) right;
            SqlRecKvTable kvTable = getKvTable(tableScan.getTable());
            if (kvTable != null) {
                return kvTable;
            }
        }

        return null;
    }

    public static boolean isKvTable(RelOptTable table) {
        return getKvTable(table) != null;
    }

    public static SqlRecKvTable getKvTable(RelOptTable table) {
        SqlRecTable sqlRecTable = null;
        if (table != null) {
            sqlRecTable = table.unwrap(SqlRecTable.class);
        }
        if (sqlRecTable != null && sqlRecTable instanceof SqlRecKvTable) {
            return (SqlRecKvTable) sqlRecTable;
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

    public static Map.Entry<Integer, Integer> getJoinIpColIndex(Join join) {
        RexNode condition = join.getCondition();

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

    public static class JoinPostProcessConfig {
        public boolean hasFindJoin = false;
        public int limit = 0;
        public List<Integer> projectColumns = new ArrayList<>();
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
}
