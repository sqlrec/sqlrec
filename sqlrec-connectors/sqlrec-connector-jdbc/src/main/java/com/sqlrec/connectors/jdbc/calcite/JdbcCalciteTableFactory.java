package com.sqlrec.connectors.jdbc.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.config.JdbcOptions;

public class JdbcCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(tableObj);
        JdbcConfig jdbcConfig = JdbcOptions.getJdbcConfig(metadata.getOptions());
        jdbcConfig.database = metadata.getDatabase();
        jdbcConfig.fieldSchemas = metadata.getFieldSchemas();
        jdbcConfig.primaryKey = metadata.getPrimaryKey();
        jdbcConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();

        return new JdbcCalciteTable(jdbcConfig);
    }

    @Override
    public String getConnectorName() {
        return JdbcOptions.CONNECTOR_IDENTIFIER;
    }
}
