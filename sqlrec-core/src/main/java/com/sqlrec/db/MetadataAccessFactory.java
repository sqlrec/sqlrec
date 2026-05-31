package com.sqlrec.db;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.SqlFileParser;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.db.remote.DbStoreAccess;
import com.sqlrec.db.remote.HmsSchemaAccess;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.sql.parser.SqlCreateService;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;

public class MetadataAccessFactory {

    private static volatile MetadataAccess instance;

    public static MetadataAccess getInstance() {
        if (instance == null) {
            synchronized (MetadataAccessFactory.class) {
                if (instance == null) {
                    try {
                        init();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return instance;
    }

    private static void init() throws Exception {
        String sqlSchemaDir = SqlRecConfigs.SQL_SCHEMA_DIR.getValue();
        if (sqlSchemaDir != null && !sqlSchemaDir.isEmpty()) {
            SqlFileParser parser = new SqlFileParser(sqlSchemaDir);
            parser.load();
            SchemaAccess schemaAccess = new SqlFileSchemaAccess(
                    parser.getTableNodes(),
                    parser.getUdfFunctionNodes()
            );
            StoreAccess storeAccess = new InMemoryStoreAccess(
                    parser.getSqlFunctionNodeGroups(),
                    parser.getApiNodes(),
                    parser.getModelNodes(),
                    new ArrayList<>()
            );
            instance = new MetadataAccess(schemaAccess, storeAccess);
            for (SqlNode node : parser.getServiceNodes()) {
                ServiceManager.saveServiceInDb((SqlCreateService) node, instance);
            }
        } else {
            instance = new MetadataAccess(new HmsSchemaAccess(), new DbStoreAccess());
        }
    }
}
