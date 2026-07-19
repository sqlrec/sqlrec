package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.utils.ObjCache;
import org.apache.calcite.jdbc.CalciteSchema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CalciteSchemaFactory {
    private static CalciteSchema globalSchema;
    private static final Map<String, HmsSchema> schemaMap = new ConcurrentHashMap<>();
    private static final ObjCache<List<String>> databaseListCache = new ObjCache<>(
            SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
            SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
            (oldDatabaseList) -> {
                try {
                    return MetadataAccessFactory.getInstance().getDatabases();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
    );

    public static void invalidateCache() {
        databaseListCache.invalidate();
        schemaMap.values().forEach(HmsSchema::invalidateCache);
    }

    public static void setGlobalSchema(CalciteSchema schema) {
        globalSchema = schema;
    }

    public static CalciteSchema createCalciteSchema() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

        if (globalSchema != null) {
            globalSchema.getSubSchemaMap().forEach((k, v) -> {
                rootSchema.add(k, v.schema);
            });
            return rootSchema;
        }

        MetadataAccess metadataAccess = MetadataAccessFactory.getInstance();
        try {
            List<String> databases = databaseListCache.getObj();
            for (String database : databases) {
                HmsSchema hmsSchema = schemaMap.computeIfAbsent(database,
                        db -> new HmsSchema(db, metadataAccess));
                rootSchema.add(database, hmsSchema);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rootSchema;
    }
}
