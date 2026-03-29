package com.sqlrec.frontend.RestService;

import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;

import java.util.ArrayList;

public class SqlExecutor {
    public static ExecuteDataList execute(String requestData) throws Exception {
        RequestData params = utils.parseRequestData(requestData);
        if (params.sqls == null || params.sqls.isEmpty()) {
            throw new Exception("sqls is null or empty");
        }

        SqlProcessor sqlProcessor = new SqlProcessor();
        sqlProcessor.setExecuteParams(params.params);

        ExecuteDataList executeDataList = new ExecuteDataList();
        executeDataList.data = new ArrayList<>();

        for (String sql : params.sqls) {
            SqlProcessResult sqlProcessResult = sqlProcessor.tryExecuteSql(sql);
            ExecuteData executeData = new ExecuteData();
            if (sqlProcessResult == null) {
                executeData.msg = "cannot execute sql: " + sql;
            } else {
                executeData.msg = sqlProcessResult.getMsg();
                executeData.data = utils.convertToMapList(
                        sqlProcessResult.getEnumerable() != null ? sqlProcessResult.getEnumerable().toList() : null,
                        sqlProcessResult.getFields()
                );
            }
            executeDataList.data.add(executeData);
        }
        return executeDataList;
    }
}
