package com.sqlrec.db;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.SqlFileParser;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.db.remote.DbStoreAccess;
import com.sqlrec.db.remote.HmsSchemaAccess;

public class MetadataAccessFactory {

    private static volatile MetadataAccess instance;

    public static MetadataAccess getInstance() {
        if (instance == null) {
            synchronized (MetadataAccessFactory.class) {
                if (instance == null) {
                    instance = createDefault();
                }
            }
        }
        return instance;
    }

    private static MetadataAccess createDefault() {
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
                    parser.getServiceNodes()
            );
            return new MetadataAccess(schemaAccess, storeAccess);
        }
        return new MetadataAccess(new HmsSchemaAccess(), new DbStoreAccess());
    }
}
