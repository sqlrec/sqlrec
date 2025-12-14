package com.sqlrec.frontend.RestService;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Map;

public class FunctionExecutor {
    public static ExecuteData execute(String apiName, String requestData) throws Exception {
        SqlFunctionBindable sqlFunctionBindable = CompileManager.getApiBindSqlFunction(apiName);
        if (sqlFunctionBindable == null) {
            throw new RuntimeException("cant find function for " + apiName);
        }
        CalciteSchema schema = HmsSchema.getHmsCalciteSchema();

        RequestData requestDataObj = utils.parseRequestData(requestData);
        addTableToSchema(schema, sqlFunctionBindable, requestDataObj.inputs);

        ExecuteContext executeContext = new ExecuteContextImpl();
        if (requestDataObj.params != null) {
            requestDataObj.params.forEach(executeContext::setVariable);
        }

        ExecuteData executeData = new ExecuteData();
        try {
            Enumerable<Object[]> enumerable = sqlFunctionBindable.bind(schema, executeContext);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                executeData.data = utils.convertToMapList(results, sqlFunctionBindable.getReturnDataFields());
            } else {
                executeData.msg = "function return null";
            }
        } catch (Exception e) {
            executeData.msg = "execute function error: " + e.getMessage();
        }
        return executeData;
    }

    private static void addTableToSchema(CalciteSchema schema, SqlFunctionBindable sqlFunctionBindable, Map<String, List<Map<String, Object>>> params) throws Exception {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
        for (Map.Entry<String, List<RelDataTypeField>> tablePlaceholder : tablePlaceholders) {
            String tableName = tablePlaceholder.getKey();
            List<RelDataTypeField> dataFields = tablePlaceholder.getValue();
            if (params == null) {
                throw new Exception("params is null, need params for table " + tableName);
            }
            if (!params.containsKey(tableName)) {
                throw new Exception("table " + tableName + " not found in params, need params for table " + tableName);
            }
            Enumerable<Object[]> enumerable = utils.convertDataToEnumerable(params.get(tableName), dataFields);
            CacheTable cacheTable = new CacheTable(tableName, enumerable, dataFields);
            schema.add(tableName, cacheTable);
        }
    }
}
