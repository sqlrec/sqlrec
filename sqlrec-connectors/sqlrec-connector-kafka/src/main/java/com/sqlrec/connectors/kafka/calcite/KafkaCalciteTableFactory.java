package com.sqlrec.connectors.kafka.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.connectors.kafka.config.KafkaOptions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaCalciteTableFactory implements HmsTableFactory {
    @Override
    public Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        KafkaConfig kafkaConfig = KafkaOptions.getKafkaConfig(flinkTableOptions);
        kafkaConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        return new KafkaCalciteTable(kafkaConfig);
    }

    @Override
    public String getConnectorName() {
        return KafkaOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return Collections.singletonList(
                KafkaEnumerableTableModifyRule.DEFAULT_CONFIG.toRule(KafkaEnumerableTableModifyRule.class)
        );
    }
}
