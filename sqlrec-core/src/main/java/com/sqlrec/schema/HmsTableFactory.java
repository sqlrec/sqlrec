package com.sqlrec.schema;

import org.apache.calcite.schema.Table;

public interface HmsTableFactory {
    Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj);
    String getConnectorName();
}
