package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.utils.CacheUtils;
import org.apache.calcite.jdbc.CalciteSchema;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class CalciteSchemaFactory {
    private static CalciteSchema globalSchema;
    private static final Map<String, HmsSchema> schemaMap = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);
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

    public static void invalidateMetadata() {
        databaseListCache.invalidate();
        schemaMap.values().forEach(HmsSchema::invalidateMetadata);
    }

    public static void evictTable(String database, String table) {
        HmsSchema schema = schemaMap.get(database);
        if (schema != null) schema.evictTable(table);
    }

    public static void evictDatabase(String database) {
        HmsSchema schema = schemaMap.get(database);
        if (schema != null) schema.invalidateCache();
    }

    public static void setGlobalSchema(CalciteSchema schema) {
        globalSchema = schema;
        databaseListCache.invalidate();
        schemaMap.clear();
    }

    /** Register a metadata database in one session, preserving custom schemas with the same name. */
    public static void addDatabase(CalciteSchema root, MetadataAccess metadata, String database) {
        if (root.getSubSchema(database, false) == null) {
            root.add(database, schemaMap.computeIfAbsent(database, name -> new HmsSchema(name, metadata)));
        }
    }

    /**
     * Uses the cached database list at session creation. Database DDL updates only its executing
     * session; other existing sessions keep their database membership.
     */
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
                addDatabase(rootSchema, MetadataAccessFactory.getInstance(), database);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rootSchema;
    }
}
