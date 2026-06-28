package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import com.sqlrec.connectors.filesystem.config.FileSystemOptions;
import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileSystemCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        FileSystemConfig fileSystemConfig = FileSystemOptions.getFileSystemConfig(flinkTableOptions);
        fileSystemConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        fileSystemConfig.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        fileSystemConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(fileSystemConfig.fieldSchemas, fileSystemConfig.primaryKey);

        return new FileSystemCalciteTable(fileSystemConfig);
    }

    @Override
    public String getConnectorName() {
        return FileSystemOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }
}
