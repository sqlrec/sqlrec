package com.sqlrec.frontend.rest;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.JsonUtils;
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

        com.sqlrec.executor.SqlExecutor sqlExecutor = new com.sqlrec.executor.SqlExecutor();
        sqlExecutor.setExecuteParams(params.getParams());

        ExecuteDataList executeDataList = new ExecuteDataList();
        executeDataList.setData(new ArrayList<>());

        for (String sql : params.getSqls()) {
            ExecuteData executeData = executeSingleSql(sqlExecutor, sql);
            executeDataList.getData().add(executeData);
        }
        return executeDataList;
    }

    private static ExecuteData executeSingleSql(com.sqlrec.executor.SqlExecutor sqlExecutor, String sql) {
        ExecuteData executeData = new ExecuteData();

        if (StringUtils.isEmpty(sql)) {
            executeData.setMsg("sql is null or empty, skip execution");
            return executeData;
        }

        try {
            CacheTable result = sqlExecutor.executeSql(sql);
            executeData.setData(DataTransformUtils.convertToMapList(
                    result.scan(null) != null ? result.scan(null).toList() : null,
                    result.getDataFields()
            ));
        } catch (Exception e) {
            executeData.setMsg("process sql error: " + e.getMessage());
        }
        return executeData;
    }
}
