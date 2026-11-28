package com.sqlrec.frontend.rest;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.ExecuteDataList;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.common.utils.SqlStatementUtils;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.frontend.utils.RestUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class RestSqlExecutor {
    public static ExecuteDataList execute(String requestBody) throws Exception {
        RequestData request = RestUtils.parseRequestData(requestBody,
                "request body is required; expected JSON with a non-empty sqls array",
                "request body must be a JSON object with a non-empty sqls array");
        List<String> sqls = request.getSqls();
        if (sqls == null || sqls.isEmpty()) {
            throw new IllegalArgumentException("sqls must be a non-empty array");
        }

        SqlExecutor sqlExecutor = new SqlExecutor();
        sqlExecutor.setExecuteParams(request.getParams());

        List<ExecuteData> results = new ArrayList<>(sqls.size());
        for (int index = 0; index < sqls.size(); index++) {
            results.add(executeSingleSql(sqlExecutor, sqls.get(index), index));
        }
        ExecuteDataList response = new ExecuteDataList();
        response.setData(results);
        return response;
    }

    private static ExecuteData executeSingleSql(SqlExecutor sqlExecutor, String sql, int index) {
        ExecuteData executeData = new ExecuteData();

        long start = System.nanoTime();
        String outcome = "success";
        try {
            if (StringUtils.isBlank(sql)) {
                outcome = "error";
                executeData.setMsg("sqls[" + index + "] is null or empty; skipped execution");
                return executeData;
            }
            CacheTable result = sqlExecutor.executeSql(sql);
            executeData.setData(RowTransformUtils.convertToMapList(
                    result.scan(null),
                    result.getDataFields()
            ));
            if (SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.getValue()) {
                executeData.setParams(sqlExecutor.getExecuteContext().getVariables());
            }
        } catch (Exception e) {
            outcome = MetricsUtils.outcome(e, sqlExecutor.getExecuteContext().isCancelled());
            executeData.setMsg("sqls[" + index + "] failed: "
                    + RestUtils.errorMessage(e, "SQL execution failed"));
        } catch (Error e) {
            outcome = "error";
            throw e;
        } finally {
            String statementType = SqlStatementUtils.firstWord(sql).toUpperCase(Locale.ROOT);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_SQL_EXECUTION_DURATION,
                            MetricsUtils.createTags(sqlExecutor.getExecuteContext().getVariables(),
                                    "statement_type", statementType, "outcome", outcome))
                    .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return executeData;
    }
}
