package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.milvus.config.MilvusOptions;
import org.apache.calcite.schema.Table;


public class MilvusCalciteTableFactory implements HmsTableFactory {
    @Override
    public Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        return new MilvusCalciteTable(MilvusOptions.fromMetadata(HiveTableUtils.getConnectorTableMetadata(tableObj)));
    }

    @Override
    public String getConnectorName() {
        return MilvusOptions.CONNECTOR_IDENTIFIER;
    }
}
