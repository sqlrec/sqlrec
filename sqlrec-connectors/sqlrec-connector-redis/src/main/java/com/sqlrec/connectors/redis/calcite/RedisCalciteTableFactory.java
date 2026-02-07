package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        RedisConfig redisConfig = RedisOptions.getRedisConfig(flinkTableOptions);
        redisConfig.database = tableObj.getDbName();
        redisConfig.tableName = tableObj.getTableName();
        redisConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        redisConfig.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        redisConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(redisConfig.fieldSchemas, redisConfig.primaryKey);

        return new RedisCalciteTable(redisConfig);
    }

    @Override
    public String getConnectorName() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }
}
