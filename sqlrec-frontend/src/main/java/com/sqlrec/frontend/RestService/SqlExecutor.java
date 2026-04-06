package com.sqlrec.frontend.RestService;

import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class SqlExecutor {
    public static ExecuteDataList execute(String requestData) throws Exception {
        if (StringUtils.isEmpty(requestData)) {
            throw new IllegalArgumentException("request data is null or empty");
        }

        RequestData params = JsonUtils.fromJson(requestData, RequestData.class);
        if (params == null) {
            throw new IllegalArgumentException("failed to parse request data");
        }

        if (params.getSqls() == null || params.getSqls().isEmpty()) {
            throw new IllegalArgumentException("sqls is null or empty");
        }

        SqlProcessor sqlProcessor = new SqlProcessor();
        sqlProcessor.setExecuteParams(params.getParams());

        ExecuteDataList executeDataList = new ExecuteDataList();
        executeDataList.setData(new ArrayList<>());

        for (String sql : params.getSqls()) {
            ExecuteData executeData = executeSingleSql(sqlProcessor, sql);
            executeDataList.getData().add(executeData);
        }
        return executeDataList;
    }

    private static ExecuteData executeSingleSql(SqlProcessor sqlProcessor, String sql) {
        ExecuteData executeData = new ExecuteData();

        if (StringUtils.isEmpty(sql)) {
            executeData.setMsg("sql is null or empty, skip execution");
            return executeData;
        }

        SqlProcessResult sqlProcessResult = sqlProcessor.tryExecuteSql(sql);
        if (sqlProcessResult == null) {
            executeData.setMsg("cannot execute sql: " + sql);
        } else {
            executeData.setMsg(sqlProcessResult.getMsg());
            executeData.setData(DataTransformUtils.convertToMapList(
                    sqlProcessResult.getEnumerable() != null ? sqlProcessResult.getEnumerable().toList() : null,
                    sqlProcessResult.getFields()
            ));
        }
        return executeData;
    }
}
