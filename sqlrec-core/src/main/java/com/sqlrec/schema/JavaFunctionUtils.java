package com.sqlrec.schema;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sqlrec.utils.CacheUtils;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class JavaFunctionUtils {
    private static final Logger log = LoggerFactory.getLogger(JavaFunctionUtils.class);
    private static volatile boolean skipHmsQuery = false;
    private static final LoadingCache<FunctionKey, Optional<Class<?>>> javaFunctionClassCache =
            createCache(
                    Duration.ofSeconds(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue()),
                    ExecutorServiceUtils.getCacheRefreshExecutorService(),
                    Ticker.systemTicker()
            );

    private static final Map<FunctionKey, Class<?>> registeredJavaFunctionClassMap = new ConcurrentHashMap<>();

    record FunctionKey(String database, String functionName) {
        FunctionKey {
            database = ResourceNames.normalize(database);
            functionName = ResourceNames.normalize(functionName);
        }
    }

    public static void invalidateCache() {
        javaFunctionClassCache.invalidateAll();
    }

    static LoadingCache<FunctionKey, Optional<Class<?>>> createCache(
            Duration refreshInterval,
            Executor executor,
            Ticker ticker
    ) {
        return CacheUtils.createRefreshCache(
                refreshInterval,
                executor,
                ticker,
                JavaFunctionUtils::loadJavaFunctionClass
        );
    }

    public static void setSkipHmsQuery(boolean skip) {
        skipHmsQuery = skip;
    }

    public static Object getTableFunction(String db, String funName) throws Exception {
        Class<?> clazz = getTableFunctionClass(db, funName, javaFunctionClassCache);
        if (clazz == null) {
            return null;
        }
        return clazz.getDeclaredConstructor().newInstance();
    }

    static Class<?> getTableFunctionClass(
            String db,
            String funName,
            LoadingCache<FunctionKey, Optional<Class<?>>> cache
    ) {
        FunctionKey key = new FunctionKey(db, funName);
        Class<?> registeredClazz = registeredJavaFunctionClassMap.get(key);
        if (registeredClazz != null) {
            return registeredClazz;
        }

        try {
            return cache.get(key).orElse(null);
        } catch (JavaFunctionLoadException e) {
            log.warn("Exception when get table function: db={}, funName={}",
                    key.database(), key.functionName(), e.getCause());
            return null;
        }
    }

    private static Optional<Class<?>> loadJavaFunctionClass(FunctionKey key) {
        try {
            String className = getJavaFunctionClassName(key);
            if (StringUtils.isEmpty(className)) {
                return Optional.empty();
            }

            Class<?> clazz = Class.forName(className);
            log.info("Register table function: db={}, funName={}, className={}",
                    key.database(), key.functionName(), className);
            return Optional.of(clazz);
        } catch (NoSuchObjectException e) {
            log.info("function: db={}, funName={} not found", key.database(), key.functionName());
            return Optional.empty();
        } catch (Exception e) {
            throw new JavaFunctionLoadException(e);
        }
    }

    private static String getJavaFunctionClassName(FunctionKey key) throws Exception {
        String builtIn = FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.get(key.functionName());
        if (builtIn != null) {
            return builtIn;
        }
        if (skipHmsQuery) {
            return null;
        }

        MetadataAccess metadataAccess = MetadataAccessFactory.getInstance();
        org.apache.hadoop.hive.metastore.api.Function functionObj =
                metadataAccess.getFunction(key.database(), key.functionName());
        if (functionObj == null) {
            throw new Exception("Function not found: " + key.functionName());
        }
        return functionObj.getClassName();
    }

    public static void registerTableFunction(String db, String funName, Class<?> clazz) {
        registeredJavaFunctionClassMap.put(new FunctionKey(db, funName), clazz);
    }

    static void unregisterTableFunction(String db, String funName) {
        registeredJavaFunctionClassMap.remove(new FunctionKey(db, funName));
    }

    private static final class JavaFunctionLoadException extends RuntimeException {
        private JavaFunctionLoadException(Exception cause) {
            super(cause);
        }
    }
}
