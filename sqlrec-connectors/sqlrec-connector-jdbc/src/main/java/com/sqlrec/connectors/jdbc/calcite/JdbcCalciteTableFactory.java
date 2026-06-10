package com.sqlrec.connectors.jdbc.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.config.JdbcOptions;
import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        JdbcConfig jdbcConfig = JdbcOptions.getJdbcConfig(flinkTableOptions);
        jdbcConfig.database = tableObj.getDbName();
        jdbcConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        jdbcConfig.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        jdbcConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(jdbcConfig.fieldSchemas, jdbcConfig.primaryKey);

        return new JdbcCalciteTable(jdbcConfig);
    }

    @Override
    public String getConnectorName() {
        return JdbcOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }
}
