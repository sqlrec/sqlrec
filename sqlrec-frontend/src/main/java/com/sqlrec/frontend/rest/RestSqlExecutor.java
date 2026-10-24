package com.sqlrec.frontend.rest;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.ExecuteDataList;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.frontend.utils.RestUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class RestSqlExecutor {
    public static ExecuteDataList execute(String requestData) throws Exception {
        if (StringUtils.isBlank(requestData)) {
            throw new IllegalArgumentException("request body is required; expected JSON with a non-empty sqls array");
        }

        RequestData params = RestUtils.parseRequestData(requestData);
        if (params == null) {
            throw new IllegalArgumentException("request body must be a JSON object with a non-empty sqls array");
        }

        if (params.getSqls() == null || params.getSqls().isEmpty()) {
            throw new IllegalArgumentException("sqls must be a non-empty array");
        }

        SqlExecutor sqlExecutor = new SqlExecutor();
        sqlExecutor.setExecuteParams(params.getParams());

        ExecuteDataList executeDataList = new ExecuteDataList();
        executeDataList.setData(new ArrayList<>());

        for (int index = 0; index < params.getSqls().size(); index++) {
            ExecuteData executeData = executeSingleSql(sqlExecutor, params.getSqls().get(index), index);
            executeDataList.getData().add(executeData);
        }
        return executeDataList;
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
            Enumerable<Object[]> rows = result.scan(null);
            executeData.setData(RowTransformUtils.convertToMapList(
                    rows == null ? null : rows.toList(),
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
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_SQL_EXECUTION_DURATION,
                            MetricsUtils.createTags(sqlExecutor.getExecuteContext().getVariables(),
                                    "statement_type", statementType(sql), "outcome", outcome))
                    .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return executeData;
    }

    private static String statementType(String sql) {
        return sql == null ? "" : sql.strip().split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
    }
}
