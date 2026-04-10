package com.sqlrec.compiler;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.JavaFunctionUtils;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FunctionUpdater {
    private static final int UPDATE_SUCCESS = 1;
    private static final int UPDATE_FAILED = -1;
    private static final int NOT_UPDATE = 0;

    private static final Logger log = LoggerFactory.getLogger(FunctionUpdater.class);
    private static ScheduledExecutorService executor;

    public static synchronized void initFunctionUpdateService() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        updateFunctionBindable();
                    } catch (Exception e) {
                        log.error("update function bindable failed", e);
                    }
                },
                SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue(),
                SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue(),
                TimeUnit.SECONDS
        );
    }

    public static void updateFunctionBindable() {
        log.info("starting function bindable update check");
        Map<String, Integer> functionUpdateStatusMap = new HashMap<>();
        Map<String, SqlFunctionBindable> functionBindableMap = CompileManager.getFunctionBindableMap();
        Set<String> functionNames = new HashSet<>(functionBindableMap.keySet());

        log.info("total functions to check: {}", functionNames.size());
        for (String functionName : functionNames) {
            tryFlushFunctionBindable(functionName, functionUpdateStatusMap, functionBindableMap);
        }
        log.info("function update check completed");
    }

    private static int tryFlushFunctionBindable(
            String functionName,
            Map<String, Integer> functionUpdateStatusMap,
            Map<String, SqlFunctionBindable> functionBindableMap
    ) {
        if (functionUpdateStatusMap.containsKey(functionName)) {
            return functionUpdateStatusMap.get(functionName);
        }

        log.info("checking function: {}", functionName);
        try {
            boolean needFlush = false;
            SqlFunctionBindable functionBindable = functionBindableMap.get(functionName);
            Set<String> dependencySqlFunctions = functionBindable.getDependencySqlFunctions();
            for (String dependencySqlFunction : dependencySqlFunctions) {
                int dependencyStatus = tryFlushFunctionBindable(
                        dependencySqlFunction, functionUpdateStatusMap, functionBindableMap
                );
                if (dependencyStatus == UPDATE_SUCCESS) {
                    log.info("dependency function {} updated, marking {} for flush", dependencySqlFunction, functionName);
                    needFlush = true;
                }
            }

            SqlFunction sqlFunction = DbUtils.getSqlFunction(functionBindable.getFunName());
            if (sqlFunction == null) {
                functionBindableMap.remove(functionName);
                functionUpdateStatusMap.put(functionName, UPDATE_SUCCESS);
                log.info("function bindable {} removed (not found in database)", functionName);
                return UPDATE_SUCCESS;
            }
            if (sqlFunction.getUpdatedAt() > functionBindable.getCreateTime()) {
                log.info("function {} updated in database (db: {}, cached: {}), marking for flush",
                        functionName, sqlFunction.getUpdatedAt(), functionBindable.getCreateTime());
                needFlush = true;
            }

            if (!needFlush) {
                needFlush = isSqlFunctionDependentResourceUpdate(functionBindable);
            }

            if (needFlush) {
                new CompileManager().compileSqlFunction(functionName);
                functionUpdateStatusMap.put(functionName, UPDATE_SUCCESS);
                log.info("function bindable {} updated", functionName);
            } else {
                functionUpdateStatusMap.put(functionName, NOT_UPDATE);
                log.info("function {} does not need update", functionName);
            }
        } catch (Exception e) {
            log.error("try flush function bindable failed : {}", functionName, e);
            functionUpdateStatusMap.put(functionName, UPDATE_FAILED);
        }

        return functionUpdateStatusMap.get(functionName);
    }

    private static boolean isSqlFunctionDependentResourceUpdate(SqlFunctionBindable functionBindable) throws Exception {
        Set<String> accessTables = functionBindable.getAccessTables();
        for (Map.Entry<String, List<RelDataTypeField>> placeholder : functionBindable.getInputTables()) {
            accessTables.remove(placeholder.getKey());
        }

        for (String accessTable : accessTables) {
            Map.Entry<String, String> dbAndTable = HiveTableUtils.getDbAndTable(accessTable);
            long lastModifiedTime = HmsSchema.getTableUpdateTime(dbAndTable.getKey(), dbAndTable.getValue());
            if (lastModifiedTime > functionBindable.getCreateTime()) {
                log.info("table {}.{} has been modified, need to flush function",
                        dbAndTable.getKey(), dbAndTable.getValue());
                return true;
            }
        }

        Set<String> javaFunctions = functionBindable.getDependencyJavaFunctions();
        for (String javaFunction : javaFunctions) {
            // todo optimize java function update check
            Object javaFunctionClass = JavaFunctionUtils.getTableFunctionClass(
                    Consts.DEFAULT_SCHEMA_NAME, javaFunction);
            long functionModificationTime = JavaFunctionUtils.getFunctionUpdateTime(
                    Consts.DEFAULT_SCHEMA_NAME, javaFunction);
            if (functionModificationTime > functionBindable.getCreateTime()) {
                log.info("java function {} has been modified, need to flush function", javaFunction);
                return true;
            }
        }

        // todo check scala udf update
        return false;
    }
}
