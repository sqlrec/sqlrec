package com.sqlrec.schema;

import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.model.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManager {
    private static final Logger log = LoggerFactory.getLogger(CacheManager.class);

    public static void invalidateAll() {
        log.info("Invalidating all caches");
        invalidate("CalciteSchemaFactory", CalciteSchemaFactory::invalidateCache);
        invalidate("JavaFunctionUtils", JavaFunctionUtils::invalidateCache);
        invalidate("SQL function", SqlFunctionCache::invalidateAll);
        invalidate("SqlApi", SqlApiCache::invalidateAll);
        invalidate("ServiceManager", ServiceManager::invalidateCache);
    }

    private static void invalidate(String name, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.error("Failed to invalidate {} cache", name, e);
        }
    }
}
