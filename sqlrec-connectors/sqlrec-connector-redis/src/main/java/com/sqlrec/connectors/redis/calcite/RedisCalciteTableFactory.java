package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.redis.config.RedisOptions;

public class RedisCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        return new RedisCalciteTable(RedisOptions.fromMetadata(HiveTableUtils.getConnectorTableMetadata(tableObj)));
    }

    @Override
    public String getConnectorName() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }
}
