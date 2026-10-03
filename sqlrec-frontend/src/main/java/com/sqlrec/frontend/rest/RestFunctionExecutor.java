package com.sqlrec.frontend.rest;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
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

        RequestData request = parseRequest(requestData);
        SqlFunctionBindable function = findFunction(apiName);
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        addTableToSchema(schema, function, request.getData());

        ExecuteContext context = createExecuteContext(request);
        return executeFunction(apiName, function, schema, context);
    }

    private static RequestData parseRequest(String requestBody) {
        if (StringUtils.isBlank(requestBody)) {
            throw new IllegalArgumentException("request body is required; expected a JSON object");
        }
        RequestData request = RestUtils.parseRequestData(requestBody);
        if (request == null) {
            throw new IllegalArgumentException("request body must be a JSON object");
        }
        return request;
    }

    private static SqlFunctionBindable findFunction(String apiName) throws Exception {
        SqlApi api = SqlApiCache.get(apiName);
        SqlFunctionBindable function = new CompileManager().getSqlFunction(api.getFunctionName());
        if (function == null) {
            throw new IllegalStateException("function '" + api.getFunctionName()
                    + "' configured for API '" + apiName + "' was not found");
        }
        return function;
    }

    private static ExecuteContext createExecuteContext(RequestData request) {
        ExecuteContext context = new ExecuteContextImpl();
        if (request.getParams() != null) {
            request.getParams().forEach(context::setVariable);
        }
        if (request.getMetricTags() != null) {
            request.getMetricTags().forEach(context::setMetricsTag);
        }
        return context;
    }

    private static ExecuteData executeFunction(
            String apiName, SqlFunctionBindable function, CalciteSchema schema, ExecuteContext context) {
        ExecuteData result = new ExecuteData();
        try {
            BindableInterface bindable = CompileManager.prepareSqlFunctionForExecution(function);
            Enumerable<Object[]> rows = bindable.bind(schema, context);
            result.setParams(context.getVariables());
            if (rows != null) {
                List<Object[]> values = rows.toList();
                result.setData(DataTransformUtils.convertToMapList(values, bindable.getReturnDataFields()));
            } else {
                result.setMsg("API '" + apiName + "' returned no result");
            }
        } catch (Exception e) {
            logger.error("execute function error", e);
            context.cancel();
            throw new RuntimeException("failed to execute API '" + apiName + "': "
                    + RestUtils.errorMessage(e, "function execution failed"), e);
        }
        return result;
    }

    private static void addTableToSchema(
            CalciteSchema schema, SqlFunctionBindable sqlFunctionBindable,
            Map<String, List<Map<String, Object>>> params) throws Exception {
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
