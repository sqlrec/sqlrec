package com.sqlrec.utils;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecKvTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

    public static RelOptTable getScanTable(RelNode aNode) {
        if (aNode instanceof RelSubset relNode) {
            List<RelNode> inputs = relNode.getRelList();
            for (RelNode input : inputs) {
                if (input instanceof TableScan tableScan) {
                    return tableScan.getTable();
                }
            }
        }

        if (aNode instanceof TableScan tableScan) {
            return tableScan.getTable();
        }

        return null;
    }

    /** Generates a table lookup in the resolved schema for an enumerable plan. */
    public static Expression tableExpression(RelOptTable table, Class<?> tableType) {
        List<String> qualifiedName = table.getQualifiedName();
        if (!(table.getRelOptSchema() instanceof CalciteCatalogReader catalogReader)) {
            throw new IllegalArgumentException("Table " + qualifiedName + " requires a CalciteCatalogReader");
        }
        CalciteSchema.TableEntry tableEntry = SqlValidatorUtil.getTableEntry(catalogReader, qualifiedName);
        if (tableEntry == null) {
            throw new IllegalArgumentException("Table not found: " + qualifiedName);
        }
        Expression lookup = Expressions.call(
                Schemas.expression(tableEntry.schema.plus()),
                BuiltInMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(tableEntry.name));
        return Expressions.convert_(lookup, tableType);
    }

    public static Map.Entry<Integer, Integer> getJoinKeyColIndex(RexNode condition) {
        List<Integer> indexList = new ArrayList<>();
        if (condition instanceof RexCall call) {
            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexInputRef inputRef) {
                        indexList.add(inputRef.getIndex());
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

    public static boolean isTrueCondition(LogicalJoin join) {
        RexNode condition = join.getCondition();
        if (condition instanceof RexLiteral literal) {
            if (literal.getValue() instanceof Boolean value) {
                return value;
            }
            if (literal.getValue() instanceof BigDecimal value) {
                return value.compareTo(BigDecimal.ONE) == 0;
            }
        }
        if (condition instanceof RexCall call) {
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

        flinkSqlNode.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                // SELECT owns its FROM tree. Collecting JOIN/AS here again would duplicate its tables.
                collectTableNames(tableReference(call), tableNames);
                return super.visit(call);
            }
        });
        return tableNames;
    }

    public static void tryGetTable(SqlNode sqlNode, List<String> tableNames) {
        collectTableNames(sqlNode instanceof SqlJoin ? sqlNode : tableReference(sqlNode), tableNames);
    }

    private static SqlNode tableReference(SqlNode node) {
        return switch (node) {
            case SqlSelect select -> select.getFrom();
            case SqlInsert insert -> insert.getTargetTable();
            case SqlUpdate update -> update.getTargetTable();
            case SqlDelete delete -> delete.getTargetTable();
            case null, default -> null;
        };
    }

    private static void collectTableNames(SqlNode sqlNode, List<String> tableNames) {
        if (sqlNode instanceof SqlIdentifier identifier) {
            tableNames.add(normalizeTableName(identifier.names));
        } else if (sqlNode instanceof SqlJoin join) {
            collectTableNames(join.getLeft(), tableNames);
            collectTableNames(join.getRight(), tableNames);
        } else if (sqlNode instanceof SqlSelect) {
            // The visitor handles nested SELECT statements independently.
            return;
        } else if (sqlNode instanceof SqlCall call) {
            if (call.getKind() == SqlKind.AS && !call.getOperandList().isEmpty()) {
                // A table alias wraps the actual table/query in operand 0. Do not visit
                // the alias identifier itself, since it is not a table dependency.
                collectTableNames(call.getOperandList().get(0), tableNames);
            }
        }
    }

    /**
     * Extracts read dependencies from a validated relational plan. Unlike the SQL
     * AST, table scans in the relational plan are unaffected by aliases and query
     * wrappers.
     */
    public static List<String> getTableFromRelNode(RelNode relNode) {
        List<String> tableNames = new ArrayList<>();
        if (relNode == null) {
            return tableNames;
        }

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan tableScan) {
                    tableNames.add(normalizeTableName(tableScan.getTable().getQualifiedName()));
                }
                super.visit(node, ordinal, parent);
            }
        }.go(relNode);
        return tableNames;
    }

    /** Extracts write targets from a validated relational plan. */
    public static List<String> getModifyTablesFromRelNode(RelNode relNode) {
        List<String> tableNames = new ArrayList<>();
        if (relNode == null) {
            return tableNames;
        }

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableModify tableModify) {
                    tableNames.add(normalizeTableName(tableModify.getTable().getQualifiedName()));
                }
                super.visit(node, ordinal, parent);
            }
        }.go(relNode);
        return tableNames;
    }

    public static String normalizeTableName(String tableName) {
        return tableName == null ? null : tableName.toLowerCase(Locale.ROOT);
    }

    private static String normalizeTableName(List<String> qualifiedName) {
        return normalizeTableName(String.join(".", qualifiedName));
    }
}
