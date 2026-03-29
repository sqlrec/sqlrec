package com.sqlrec.frontend.common;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;

public class SqlProcessResult {
    private THandleIdentifier handleIdentifier;
    private String queryId;
    private String msg;
    private Enumerable<Object[]> enumerable;
    private List<RelDataTypeField> fields;
    private Exception exception;

    public SqlProcessResult() {

    }

    public SqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            THandleIdentifier handleIdentifier,
            String queryId,
            String msg
    ) {
        this.enumerable = enumerable;
        this.fields = fields;
        this.handleIdentifier = handleIdentifier;
        this.queryId = queryId;
        this.msg = msg;
    }

    public THandleIdentifier getHandleIdentifier() {
        return handleIdentifier;
    }

    public void setHandleIdentifier(THandleIdentifier handleIdentifier) {
        this.handleIdentifier = handleIdentifier;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Enumerable<Object[]> getEnumerable() {
        return enumerable;
    }

    public void setEnumerable(Enumerable<Object[]> enumerable) {
        this.enumerable = enumerable;
    }

    public List<RelDataTypeField> getFields() {
        return fields;
    }

    public void setFields(List<RelDataTypeField> fields) {
        this.fields = fields;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public boolean isCompleted() {
        return true;
    }
}
