package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.utils.CacheUtils;
import org.apache.calcite.jdbc.CalciteSchema;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CalciteSchemaFactory {
    private static CalciteSchema globalSchema;
    private static final Map<String, HmsSchema> schemaMap = new ConcurrentHashMap<>();
    private static final CacheUtils.SingleValueCache<List<String>> databaseListCache =
            CacheUtils.createSingleValueRefreshCache(
                    Duration.ofSeconds(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue()),
                    ignored -> {
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
        CalciteSchema rootSchema = ConcurrentCalciteSchema.createRootSchema();

        if (globalSchema != null) {
            globalSchema.getSubSchemaMap().forEach((k, v) -> {
                rootSchema.add(k, v.schema);
            });
            return rootSchema;
        }

        try {
            List<String> databases = databaseListCache.get();
            for (String database : databases) {
                HmsSchema hmsSchema = schemaMap.computeIfAbsent(database,
                        db -> new HmsSchema(db, MetadataAccessFactory.getInstance()));
                rootSchema.add(database, hmsSchema);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rootSchema;
    }
}
