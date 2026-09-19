package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class TableFactoryUtils {
    private static final Logger log = LoggerFactory.getLogger(TableFactoryUtils.class);

    public static Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        try {
            String connector = HiveTableUtils.getTableConnector(tableObj);
            if (connector == null) {
                log.info("Table {} has null connector, skip", tableObj.getTableName());
                return null;
            }
            HmsTableFactory tableFactory = getTableFactory(connector);
            if (tableFactory != null) {
                Table table = tableFactory.getTableFromHmsTable(tableObj);
                if (table instanceof SqlRecTable) {
                    String dbName = tableObj.getDbName();
                    String tableName = tableObj.getTableName();
                    String fullName = Consts.DEFAULT_SCHEMA_NAME.equals(dbName)
                            ? tableName
                            : dbName + "." + tableName;
                    ((SqlRecTable) table).setTableName(fullName);
                }
                return table;
            } else {
                log.info("Table {} connector {} factory is null, skip", tableObj.getTableName(), connector);
            }
        } catch (Exception e) {
            log.error("Error while getting table from hms table {}", tableObj.getTableName(), e);
        }
        return null;
    }

    public static HmsTableFactory getTableFactory(String connector) {
        return getTableFactoryMap().get(connector);
    }

    public static Map<String, HmsTableFactory> getTableFactoryMap() {
        return FactoryHolder.FACTORIES;
    }

    private static final class FactoryHolder {
        private static final Map<String, HmsTableFactory> FACTORIES = loadFactories();

        private static Map<String, HmsTableFactory> loadFactories() {
            Map<String, HmsTableFactory> factories = new HashMap<>();
            for (HmsTableFactory tableFactory : ServiceLoader.load(HmsTableFactory.class)) {
                factories.put(tableFactory.getConnectorName(), tableFactory);
            }
            return Collections.unmodifiableMap(factories);
        }
    }
}
