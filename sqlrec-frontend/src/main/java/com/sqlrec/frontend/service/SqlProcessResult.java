package com.sqlrec.frontend.service;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;

public class SqlProcessResult {
    public THandleIdentifier handleIdentifier;
    public String queryId;
    public String msg;
    public Enumerable<Object[]> enumerable;
    public List<RelDataTypeField> fields;
    public Exception exception;

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
}
