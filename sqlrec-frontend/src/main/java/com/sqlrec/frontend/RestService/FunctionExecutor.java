package com.sqlrec.frontend.RestService;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(FunctionExecutor.class);

    public static ExecuteData execute(String apiName, String requestData) throws Exception {
        if (StringUtils.isEmpty(apiName)) {
            throw new IllegalArgumentException("apiName is null or empty");
        }

        SqlFunctionBindable sqlFunctionBindable = CompileManager.getApiBindSqlFunction(apiName);
        if (sqlFunctionBindable == null) {
            throw new IllegalArgumentException("function not found for api: " + apiName);
        }

        CalciteSchema schema = HmsSchema.getHmsCalciteSchema();

        RequestData requestDataObj = JsonUtils.fromJson(requestData, RequestData.class);
        addTableToSchema(schema, sqlFunctionBindable, requestDataObj.getInputs());

        ExecuteContext executeContext = new ExecuteContextImpl();
        if (requestDataObj.getParams() != null) {
            requestDataObj.getParams().forEach(executeContext::setVariable);
        }

        ExecuteData executeData = new ExecuteData();
        try {
            Enumerable<Object[]> enumerable = sqlFunctionBindable.bind(schema, executeContext);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                executeData.setData(DataTransformUtils.convertToMapList(results, sqlFunctionBindable.getReturnDataFields()));
            } else {
                executeData.setMsg("function return null");
            }
        } catch (Exception e) {
            logger.error("execute function error", e);
            throw new RuntimeException("execute function error: " + e.getMessage(), e);
        }
        return executeData;
    }

    private static void addTableToSchema(CalciteSchema schema, SqlFunctionBindable sqlFunctionBindable, Map<String, List<Map<String, Object>>> params) throws Exception {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
        for (Map.Entry<String, List<RelDataTypeField>> tablePlaceholder : tablePlaceholders) {
            String tableName = tablePlaceholder.getKey();
            List<RelDataTypeField> dataFields = tablePlaceholder.getValue();
            
            if (params == null) {
                throw new IllegalArgumentException("params is null, need params for table: " + tableName);
            }
            if (!params.containsKey(tableName)) {
                throw new IllegalArgumentException("table '" + tableName + "' not found in params");
            }
            
            Enumerable<Object[]> enumerable = DataTransformUtils.convertDataToEnumerable(params.get(tableName), dataFields);
            CacheTable cacheTable = new CacheTable(tableName, enumerable, dataFields);
            schema.add(tableName, cacheTable);
        }
    }
}
