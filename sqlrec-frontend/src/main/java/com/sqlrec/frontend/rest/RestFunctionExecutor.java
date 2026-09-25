package com.sqlrec.frontend.rest;

import com.google.gson.JsonParseException;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.frontend.utils.RestUtils;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class RestFunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RestFunctionExecutor.class);

    public static ExecuteData execute(String apiName, String requestData) throws Exception {
        if (StringUtils.isBlank(apiName)) {
            throw new IllegalArgumentException("API name is required");
        }
        apiName = ResourceNames.normalize(apiName);

        if (StringUtils.isBlank(requestData)) {
            throw new IllegalArgumentException("request body is required; expected a JSON object");
        }
        RequestData requestDataObj;
        try {
            requestDataObj = JsonUtils.fromJson(requestData, RequestData.class);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("invalid JSON request body: "
                    + RestUtils.errorMessage(e, "could not parse JSON"), e);
        }
        if (requestDataObj == null) {
            throw new IllegalArgumentException("request body must be a JSON object");
        }

        SqlApi sqlApi = SqlApiCache.get(apiName);
        SqlFunctionBindable sqlFunctionBindable =
                new CompileManager().getSqlFunction(sqlApi.getFunctionName());
        if (sqlFunctionBindable == null) {
            throw new IllegalStateException("function '" + sqlApi.getFunctionName()
                    + "' configured for API '" + apiName + "' was not found");
        }

        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();

        addTableToSchema(schema, sqlFunctionBindable, requestDataObj.getData());

        ExecuteContext executeContext = new ExecuteContextImpl();
        if (requestDataObj.getParams() != null) {
            requestDataObj.getParams().forEach(executeContext::setVariable);
        }
        if (requestDataObj.getMetricTags() != null) {
            requestDataObj.getMetricTags().forEach(executeContext::setMetricsTag);
        }

        ExecuteData executeData = new ExecuteData();
        try {
            BindableInterface proxyBindable = CompileManager.prepareSqlFunctionForExecution(sqlFunctionBindable);
            Enumerable<Object[]> enumerable = proxyBindable.bind(schema, executeContext);
            executeData.setParams(executeContext.getVariables());
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                executeData.setData(DataTransformUtils.convertToMapList(results, proxyBindable.getReturnDataFields()));
            } else {
                executeData.setMsg("API '" + apiName + "' returned no result");
            }
        } catch (Exception e) {
            logger.error("execute function error", e);
            executeContext.cancel();
            throw new RuntimeException("failed to execute API '" + apiName + "': "
                    + RestUtils.errorMessage(e, "function execution failed"), e);
        }
        return executeData;
    }

    private static void addTableToSchema(CalciteSchema schema, SqlFunctionBindable sqlFunctionBindable, Map<String, List<Map<String, Object>>> params) throws Exception {
        List<Map.Entry<String, List<RelDataTypeField>>> tablePlaceholders = sqlFunctionBindable.getInputTables();
        for (Map.Entry<String, List<RelDataTypeField>> tablePlaceholder : tablePlaceholders) {
            String tableName = tablePlaceholder.getKey();
            List<RelDataTypeField> dataFields = tablePlaceholder.getValue();

            if (params == null) {
                throw new IllegalArgumentException("data is required for input table '" + tableName + "'");
            }
            if (!params.containsKey(tableName)) {
                throw new IllegalArgumentException("input table '" + tableName + "' is missing from data");
            }

            Enumerable<Object[]> enumerable = DataTransformUtils.convertDataToEnumerable(params.get(tableName), dataFields);
            CacheTable cacheTable = new CacheTable(tableName, enumerable, dataFields);
            schema.add(tableName, cacheTable);
        }
    }
}
