package com.sqlrec.utils;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecKvTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class KvTableUtils {
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

    public static boolean isKvTable(RelOptTable table) {
        if (table == null) {
            return false;
        }
        return table.unwrap(SqlRecKvTable.class) != null;
    }

}
