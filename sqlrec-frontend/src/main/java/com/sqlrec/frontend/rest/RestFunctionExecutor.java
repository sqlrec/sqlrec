package com.sqlrec.frontend.rest;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
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
import java.util.concurrent.TimeUnit;

public class RestFunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RestFunctionExecutor.class);

    public static ExecuteData execute(String apiName, String requestBody) throws Exception {
        if (StringUtils.isBlank(apiName)) {
            throw new IllegalArgumentException("API name is required");
        }
        apiName = ResourceNames.normalize(apiName);

        RequestData request = RestUtils.parseRequestData(requestBody,
                "request body is required; expected a JSON object", "request body must be a JSON object");
        SqlFunctionBindable function = findFunction(apiName);
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        addTableToSchema(schema, function, request.getData());

        ExecuteContext context = createExecuteContext(request, schema);
        return executeFunction(apiName, function, schema, context);
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

    private static ExecuteContext createExecuteContext(RequestData request, CalciteSchema schema) {
        ExecuteContext context = new ExecuteContextImpl(schema);
        if (request.getParams() != null) {
            request.getParams().forEach(context::setVariable);
        }
        return context;
    }

    private static ExecuteData executeFunction(
            String apiName, SqlFunctionBindable function, CalciteSchema schema, ExecuteContext context) {
        ExecuteData result = new ExecuteData();
        long start = System.nanoTime();
        String outcome = "success";
        try {
            BindableInterface bindable = CompileManager.prepareSqlFunctionForExecution(function);
            Enumerable<Object[]> rows = bindable.bind(schema, context);
            if (SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.getValue()) {
                result.setParams(context.getVariables());
            }
            if (rows != null) {
                result.setData(RowTransformUtils.convertToMapList(rows, bindable.getReturnDataFields(), true));
            } else {
                result.setMsg("API '" + apiName + "' returned no result");
            }
        } catch (Exception e) {
            outcome = MetricsUtils.outcome(e, context.isCancelled());
            logger.error("execute function error", e);
            context.cancel();
            throw new RuntimeException("failed to execute API '" + apiName + "': "
                    + RestUtils.errorMessage(e, "function execution failed"), e);
        } catch (Error e) {
            outcome = "error";
            throw e;
        } finally {
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_API_EXECUTION_DURATION,
                            MetricsUtils.createTags(context.getVariables(), "api", apiName, "outcome", outcome))
                    .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
            List<Map<String, Object>> rows = params.get(tableName);
            if (rows == null) {
                String reason = params.containsKey(tableName) ? " must be an array" : " is missing from data";
                throw new IllegalArgumentException("input table '" + tableName + "'" + reason);
            }

            Enumerable<Object[]> enumerable = RowTransformUtils.convertDataToEnumerable(rows, dataFields);
            CacheTable cacheTable = new CacheTable(tableName, enumerable, dataFields);
            schema.add(tableName, cacheTable);
        }
    }
}
