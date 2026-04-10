package com.sqlrec.schema;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class TableFactoryUtils {
    private static final Logger log = LoggerFactory.getLogger(TableFactoryUtils.class);

    private static Map<String, HmsTableFactory> tableFactories;

    public static Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        try {
            String connector = HiveTableUtils.getTableConnector(tableObj);
            if (connector == null) {
                log.warn("Table {} has null connector, skip", tableObj.getTableName());
                return null;
            }
            HmsTableFactory tableFactory = getTableFactory(connector);
            if (tableFactory != null) {
                return tableFactory.getTableFromHmsTable(tableObj);
            } else {
                log.warn("Table {} connector {} factory is null, skip", tableObj.getTableName(), connector);
            }
        } catch (Exception e) {
            log.error("Error while getting table from hms table {}", tableObj.getTableName(), e);
        }
        return null;
    }

    public static HmsTableFactory getTableFactory(String connector) {
        if (tableFactories == null) {
            getTableFactoryMap();
        }
        return tableFactories.getOrDefault(connector, null);
    }

    public static synchronized Map<String, HmsTableFactory> getTableFactoryMap() {
        if (tableFactories == null) {
            tableFactories = new ConcurrentHashMap<>();
            ServiceLoader<HmsTableFactory> serviceLoader = ServiceLoader.load(HmsTableFactory.class);
            for (HmsTableFactory tableFactory : serviceLoader) {
                tableFactories.put(tableFactory.getConnectorName(), tableFactory);
            }
        }
        return tableFactories;
    }
}
