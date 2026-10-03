package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;

public class RedisCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(tableObj);
        RedisConfig redisConfig = RedisOptions.getRedisConfig(metadata.getOptions());
        redisConfig.database = metadata.getDatabase();
        redisConfig.tableName = metadata.getTableName();
        redisConfig.fieldSchemas = metadata.getFieldSchemas();
        redisConfig.primaryKey = metadata.getPrimaryKey();
        redisConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();

        return new RedisCalciteTable(redisConfig);
    }

    @Override
    public String getConnectorName() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }
}
