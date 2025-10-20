package com.sqlrec.utils;

import com.sqlrec.schema.HmsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableFunctionUtils {
    private static final Logger log = LoggerFactory.getLogger(TableFunctionUtils.class);
    private static Map<String, Class<?>> tableFunctionClassMap = new ConcurrentHashMap<>();

    public static Object getTableFunction(String db, String funName) throws Exception {
        Class<?> clazz = tableFunctionClassMap.get(db + "." + funName);
        if (clazz == null) {
            try {
                org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(db, funName);
                if (functionObj == null) {
                    return null;
                }
                clazz = Class.forName(functionObj.getClassName());
                tableFunctionClassMap.put(db + "." + funName, clazz);
            } catch (Exception e) {
                log.warn("Exception when get table function: db={}, funName={}", db, funName, e);
                return null;
            }
        }

        // for test
        if (clazz.isPrimitive()) {
            return null;
        }

        return clazz.getDeclaredConstructor().newInstance();
    }

    public static void registerTableFunction(String db, String funName, Class<?> clazz) {
        tableFunctionClassMap.put(db + "." + funName, clazz);
    }
}
