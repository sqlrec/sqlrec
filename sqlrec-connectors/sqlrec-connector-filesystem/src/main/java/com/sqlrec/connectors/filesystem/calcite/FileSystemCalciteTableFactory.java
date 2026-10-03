package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import com.sqlrec.connectors.filesystem.config.FileSystemOptions;

public class FileSystemCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        ConnectorTableMetadata metadata = HiveTableUtils.getConnectorTableMetadata(tableObj);
        FileSystemConfig fileSystemConfig = FileSystemOptions.getFileSystemConfig(metadata.getOptions());
        fileSystemConfig.fieldSchemas = metadata.getFieldSchemas();
        fileSystemConfig.primaryKey = metadata.getPrimaryKey();
        fileSystemConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();

        return new FileSystemCalciteTable(fileSystemConfig);
    }

    @Override
    public String getConnectorName() {
        return FileSystemOptions.CONNECTOR_IDENTIFIER;
    }
}
