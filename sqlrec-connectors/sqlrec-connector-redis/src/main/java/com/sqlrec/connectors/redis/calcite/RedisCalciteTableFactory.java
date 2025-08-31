package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.utils.FieldSchema;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import com.sqlrec.schema.HmsTableFactory;
import com.sqlrec.utils.HiveTableUtils;
import org.apache.calcite.plan.RelOptRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        RedisConfig redisConfig = RedisOptions.getRedisConfig(flinkTableOptions);
        List<FieldSchema> fieldSchemas = HiveTableUtils.parse(tableObj);
        if (fieldSchemas == null || fieldSchemas.isEmpty()) {
            return null;
        }
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
