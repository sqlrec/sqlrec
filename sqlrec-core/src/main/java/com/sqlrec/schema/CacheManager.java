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

        try {
            CalciteSchemaFactory.invalidateCache();
        } catch (Exception e) {
            log.error("Failed to invalidate CalciteSchemaFactory cache", e);
        }

        try {
            JavaFunctionUtils.invalidateCache();
        } catch (Exception e) {
            log.error("Failed to invalidate JavaFunctionUtils cache", e);
        }

        try {
            SqlFunctionCache.invalidateAll();
        } catch (Exception e) {
            log.error("Failed to invalidate SQL function cache", e);
        }

        try {
            SqlApiCache.invalidateAll();
        } catch (Exception e) {
            log.error("Failed to invalidate SqlApi cache", e);
        }

        try {
            ServiceManager.invalidateCache();
        } catch (Exception e) {
            log.error("Failed to invalidate ServiceManager cache", e);
        }
    }
}
