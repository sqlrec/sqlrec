package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.config.MilvusOptions;
import org.apache.calcite.schema.Table;


public class MilvusCalciteTableFactory implements HmsTableFactory {
    @Override
    public Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(tableObj);
        MilvusConfig milvusConfig = MilvusOptions.getMilvusConfig(metadata.getOptions());
        milvusConfig.fieldSchemas = metadata.getFieldSchemas();
        milvusConfig.primaryKey = metadata.getPrimaryKey();
        milvusConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();

        return new MilvusCalciteTable(milvusConfig);
    }

    @Override
    public String getConnectorName() {
        return MilvusOptions.CONNECTOR_IDENTIFIER;
    }
}
