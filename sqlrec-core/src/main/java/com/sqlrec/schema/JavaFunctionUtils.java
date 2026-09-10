package com.sqlrec.schema;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sqlrec.utils.CacheUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class JavaFunctionUtils {
    private static final Logger log = LoggerFactory.getLogger(JavaFunctionUtils.class);
    private static volatile boolean skipHmsQuery = false;
    private static final LoadingCache<String, Optional<Class<?>>> javaFunctionClassCache =
            CacheUtils.createRefreshCache(
                    Duration.ofSeconds(SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue()),
                    JavaFunctionUtils::loadJavaFunctionClass
            );

    private static final Map<String, Class<?>> registeredJavaFunctionClassMap = new ConcurrentHashMap<>();

    public static void invalidateCache() {
        javaFunctionClassCache.invalidateAll();
    }

    public static void setSkipHmsQuery(boolean skip) {
        skipHmsQuery = skip;
    }

    public static boolean isSkipHmsQuery() {
        return skipHmsQuery;
    }

    public static Object getTableFunction(String db, String funName) throws Exception {
        Class<?> clazz = getTableFunctionClass(db, funName);
        if (clazz == null) {
            return null;
        }
        return clazz.getDeclaredConstructor().newInstance();
    }

    public static Class<?> getTableFunctionClass(String db, String funName) {
        String normalizedDb = ResourceNames.normalize(db);
        String normalizedFunName = ResourceNames.normalize(funName);
        String mapKey = getMapKey(normalizedDb, normalizedFunName);
        Class<?> registeredClazz = registeredJavaFunctionClassMap.get(mapKey);
        if (registeredClazz != null) {
            return registeredClazz;
        }

        try {
            return javaFunctionClassCache.get(mapKey).orElse(null);
        } catch (JavaFunctionLoadException e) {
            log.warn("Exception when get table function: db={}, funName={}",
                    normalizedDb, normalizedFunName, e.getCause());
            return null;
        }
    }

    private static Optional<Class<?>> loadJavaFunctionClass(String mapKey) {
        int separatorIndex = mapKey.indexOf('.');
        String db = mapKey.substring(0, separatorIndex);
        String funName = mapKey.substring(separatorIndex + 1);
        try {
            String className = getJavaFunctionClassName(db, funName);
            if (StringUtils.isEmpty(className)) {
                return Optional.empty();
            }

            Class<?> clazz = Class.forName(className);
            log.info("Register table function: db={}, funName={}, className={}", db, funName, className);
            return Optional.of(clazz);
        } catch (NoSuchObjectException e) {
            log.info("function: db={}, funName={} not found", db, funName);
            return Optional.empty();
        } catch (Exception e) {
            throw new JavaFunctionLoadException(e);
        }
    }

    public static String getJavaFunctionClassName(String db, String funName) throws Exception {
        db = ResourceNames.normalize(db);
        funName = ResourceNames.normalize(funName);
        if (FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.containsKey(funName)) {
            return FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.get(funName);
        }
        if (skipHmsQuery) {
            return null;
        }

        MetadataAccess metadataAccess = MetadataAccessFactory.getInstance();
        org.apache.hadoop.hive.metastore.api.Function functionObj = metadataAccess.getFunction(db, funName);
        if (functionObj == null) {
            throw new Exception("Function not found: " + funName);
        }
        return functionObj.getClassName();
    }

    public static void registerTableFunction(String db, String funName, Class<?> clazz) {
        registeredJavaFunctionClassMap.put(
                getMapKey(ResourceNames.normalize(db), ResourceNames.normalize(funName)),
                clazz
        );
    }

    private static String getMapKey(String db, String funName) {
        return db + "." + funName;
    }

    private static final class JavaFunctionLoadException extends RuntimeException {
        private JavaFunctionLoadException(Exception cause) {
            super(cause);
        }
    }
}
