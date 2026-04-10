package com.sqlrec.utils;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecKvTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeUtils {
    public static boolean isScanKVTable(RelNode relNode) {
        return getScanKVTable(relNode) != null;
    }

    public static SqlRecKvTable getScanKVTable(RelNode relNode) {
        RelOptTable table = getScanTable(relNode);
        if (table == null) {
            return null;
        }
        return table.unwrap(SqlRecKvTable.class);
    }

    public static CacheTable getScanCacheTable(RelNode relNode) {
        RelOptTable table = getScanTable(relNode);
        if (table == null) {
            return null;
        }
        return table.unwrap(CacheTable.class);
    }

    public static boolean isKvTable(RelOptTable table) {
        if (table == null) {
            return false;
        }
        return table.unwrap(SqlRecKvTable.class) != null;
    }

    public static RelOptTable getScanTable(RelNode aNode) {
        if (aNode instanceof RelSubset) {
            RelSubset relNode = ((RelSubset) aNode);
            List<RelNode> inputs = relNode.getRelList();
            for (RelNode input : inputs) {
                if (input instanceof TableScan) {
                    TableScan tableScan = (TableScan) input;
                    return tableScan.getTable();
                }
            }
        }

        if (aNode instanceof TableScan) {
            TableScan tableScan = (TableScan) aNode;
            return tableScan.getTable();
        }

        return null;
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

    public static List<String> getTableFromSqlNode(SqlNode flinkSqlNode) {
        List<String> tableNames = new ArrayList<>();
        if (flinkSqlNode == null) {
            return tableNames;
        }

        class TableNameVisitor extends SqlBasicVisitor<Void> {
            @Override
            public Void visit(SqlCall call) {
                tryGetTable(call, tableNames);
                return super.visit(call);
            }
        }

        TableNameVisitor visitor = new TableNameVisitor();
        flinkSqlNode.accept(visitor);

        return tableNames;
    }

    public static void tryGetTable(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode == null) {
            return;
        }

        if (sqlNode instanceof SqlInsert) {
            SqlInsert insertSql = (SqlInsert) sqlNode;
            tryGetTableNameFromSqlNode(insertSql.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            tryGetTableNameFromSqlNode(sqlSelect.getFrom(), tableNames);
        } else if (sqlNode instanceof SqlUpdate) {
            SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
            tryGetTableNameFromSqlNode(sqlUpdate.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlDelete) {
            SqlDelete sqlDelete = (SqlDelete) sqlNode;
            tryGetTableNameFromSqlNode(sqlDelete.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlKind = (SqlJoin) sqlNode;
            tryGetTableNameFromSqlNode(sqlKind.getLeft(), tableNames);
            tryGetTableNameFromSqlNode(sqlKind.getRight(), tableNames);
        }
    }

    private static void tryGetTableNameFromSqlNode(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            String tableName = sqlIdentifier.toString();
            tableNames.add(tableName);
        }
    }

    public static List<String> getModifyTablesFromSqlNode(SqlNode flinkSqlNode) {
        List<String> tableNames = new ArrayList<>();
        if (flinkSqlNode == null) {
            return tableNames;
        }

        class ModifyTableNameVisitor extends SqlBasicVisitor<Void> {
            @Override
            public Void visit(SqlCall call) {
                tryGetModifyTablesFromSqlNode(call, tableNames);
                return super.visit(call);
            }
        }

        ModifyTableNameVisitor visitor = new ModifyTableNameVisitor();
        flinkSqlNode.accept(visitor);

        return tableNames;
    }

    public static void tryGetModifyTablesFromSqlNode(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode == null) {
            return;
        }

        if (sqlNode instanceof SqlInsert) {
            SqlInsert insertSql = (SqlInsert) sqlNode;
            tryGetTableNameFromSqlNode(insertSql.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlUpdate) {
            SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
            tryGetTableNameFromSqlNode(sqlUpdate.getTargetTable(), tableNames);
        } else if (sqlNode instanceof SqlDelete) {
            SqlDelete sqlDelete = (SqlDelete) sqlNode;
            tryGetTableNameFromSqlNode(sqlDelete.getTargetTable(), tableNames);
        }
    }
}
