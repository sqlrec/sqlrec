package com.sqlrec.frontend.rest;

import com.google.gson.JsonParseException;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.ExecuteDataList;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.frontend.utils.RestUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class RestSqlExecutor {
    public static ExecuteDataList execute(String requestData) throws Exception {
        if (StringUtils.isBlank(requestData)) {
            throw new IllegalArgumentException("request body is required; expected JSON with a non-empty sqls array");
        }

        RequestData params;
        try {
            params = JsonUtils.fromJson(requestData, RequestData.class);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("invalid JSON request body: "
                    + RestUtils.errorMessage(e, "could not parse JSON"), e);
        }
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

        if (StringUtils.isBlank(sql)) {
            executeData.setMsg("sqls[" + index + "] is null or empty; skipped execution");
            return executeData;
        }

        try {
            CacheTable result = sqlExecutor.executeSql(sql);
            Enumerable<Object[]> rows = result.scan(null);
            executeData.setData(DataTransformUtils.convertToMapList(
                    rows == null ? null : rows.toList(),
                    result.getDataFields()
            ));
            executeData.setParams(sqlExecutor.getExecuteContext().getVariables());
        } catch (Exception e) {
            executeData.setMsg("sqls[" + index + "] failed: "
                    + RestUtils.errorMessage(e, "SQL execution failed"));
        }
        return executeData;
    }
}
