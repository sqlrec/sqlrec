package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.connectors.redis.config.FieldSchema;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import com.sqlrec.schema.HmsTableFactory;
import org.apache.calcite.plan.RelOptRule;

import java.util.Collections;
import java.util.List;

public class RedisCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        RedisConfig redisConfig = RedisOptions.getRedisConfig(tableObj.getParameters());
        List<FieldSchema> fieldSchemas = FieldSchema.parse(tableObj.getSd().getCols());
        return new RedisCalciteTable(redisConfig, fieldSchemas);
    }

    @Override
    public String getConnectorName() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return Collections.singletonList(RedisEnumerableTableModifyRule.DEFAULT_CONFIG.toRule(RedisEnumerableTableModifyRule.class));
    }
}
