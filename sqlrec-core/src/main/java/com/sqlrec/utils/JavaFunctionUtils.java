package com.sqlrec.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sqlrec.common.config.FunctionConfigs;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.schema.HmsClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class JavaFunctionUtils {
    private static final Logger log = LoggerFactory.getLogger(JavaFunctionUtils.class);
    private static Map<String, Class<?>> javaFunctionClassMap = new ConcurrentHashMap<>();
    private static Map<String, Long> functionUpdateTime = new ConcurrentHashMap<>();
    private static Cache<String, String> notExistCache = Caffeine.newBuilder()
            .expireAfterWrite(SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue(), TimeUnit.SECONDS)
            .build();

    public static Object getTableFunction(String db, String funName) throws Exception {
        String mapKey = getMapKey(db, funName);
        Class<?> clazz = javaFunctionClassMap.get(mapKey);
        if (clazz == null) {
            clazz = getTableFunctionClass(db, funName);
        }
        if (clazz == null) {
            return null;
        }
        // for test
        if (clazz.isPrimitive()) {
            return null;
        }

        return clazz.getDeclaredConstructor().newInstance();
    }

    public static Class<?> getTableFunctionClass(String db, String funName) {
        String mapKey = getMapKey(db, funName);
        if (notExistCache.asMap().containsKey(mapKey)) {
            return null;
        }

        Class<?> clazz = null;
        try {
            String className = getJavaFunctionClassName(db, funName);
            if (!javaFunctionClassMap.containsKey(mapKey) ||
                    !javaFunctionClassMap.get(mapKey).getName().equals(className)) {
                clazz = Class.forName(className);
                javaFunctionClassMap.put(mapKey, clazz);
                functionUpdateTime.put(mapKey, System.currentTimeMillis());
                log.info("Register table function: db={}, funName={}, className={}", db, funName, className);
            } else {
                clazz = javaFunctionClassMap.get(mapKey);
            }
        } catch (NoSuchObjectException e) {
            log.info("function: db={}, funName={} not found", db, funName);
            notExistCache.put(mapKey, "");
            return null;
        } catch (Exception e) {
            log.warn("Exception when get table function: db={}, funName={}", db, funName, e);
            return null;
        }
        return clazz;
    }

    public static String getJavaFunctionClassName(String db, String funName) throws Exception {
        if (FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.containsKey(funName)) {
            return FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS.get(funName);
        }
        org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(db, funName);
        if (functionObj == null) {
            throw new Exception("Function not found: " + funName);
        }
        return functionObj.getClassName();
    }

    public static void registerTableFunction(String db, String funName, Class<?> clazz) {
        javaFunctionClassMap.put(getMapKey(db, funName), clazz);
    }

    public static long getFunctionUpdateTime(String db, String funName) {
        String mapKey = getMapKey(db, funName);
        if (functionUpdateTime.containsKey(mapKey)) {
            return functionUpdateTime.get(mapKey);
        } else {
            return 0;
        }
    }

    private static String getMapKey(String db, String funName) {
        return db + "." + funName;
    }
}
