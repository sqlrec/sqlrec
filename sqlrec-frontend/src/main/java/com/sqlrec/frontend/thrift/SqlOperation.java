package com.sqlrec.frontend.thrift;

import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;

public class SqlOperation {
    private final SqlProcessResult coreResult;
    private final THandleIdentifier handleIdentifier;
    private final String queryId;
    private String msg;
    private Exception exception;

    public SqlOperation(SqlProcessResult coreResult, THandleIdentifier handleIdentifier, String queryId) {
        this.coreResult = coreResult;
        this.handleIdentifier = handleIdentifier;
        this.queryId = queryId;
    }

    public Enumerable<Object[]> getEnumerable() {
        return coreResult.getEnumerable();
    }

    public void setEnumerable(Enumerable<Object[]> enumerable) {
        coreResult.setEnumerable(enumerable);
    }

    public List<RelDataTypeField> getFields() {
        return coreResult.getFields();
    }

    public boolean isCompleted() {
        return coreResult.isCompleted();
    }

    public THandleIdentifier getHandleIdentifier() {
        return handleIdentifier;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
