package com.sqlrec.connectors.mongodb.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.config.MongoOptions;

public class MongoCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(tableObj);
        MongoConfig mongoConfig = MongoOptions.getMongoConfig(metadata.getOptions());
        mongoConfig.fieldSchemas = metadata.getFieldSchemas();
        mongoConfig.primaryKey = metadata.getPrimaryKey();
        mongoConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();

        return new MongoCalciteTable(mongoConfig);
    }

    @Override
    public String getConnectorName() {
        return MongoOptions.CONNECTOR_IDENTIFIER;
    }
}
