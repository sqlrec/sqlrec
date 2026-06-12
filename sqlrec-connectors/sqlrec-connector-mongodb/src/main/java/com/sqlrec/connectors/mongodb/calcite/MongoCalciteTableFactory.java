package com.sqlrec.connectors.mongodb.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.config.MongoOptions;
import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        MongoConfig mongoConfig = MongoOptions.getMongoConfig(flinkTableOptions);
        mongoConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        mongoConfig.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        mongoConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(mongoConfig.fieldSchemas, mongoConfig.primaryKey);

        return new MongoCalciteTable(mongoConfig);
    }

    @Override
    public String getConnectorName() {
        return MongoOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }
}
