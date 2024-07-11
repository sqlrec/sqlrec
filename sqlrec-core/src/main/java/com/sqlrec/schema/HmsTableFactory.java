package com.sqlrec.schema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.Table;

import java.util.List;

public interface HmsTableFactory {
    Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj);
    String getConnectorName();
    List<RelOptRule> getRules();
}
