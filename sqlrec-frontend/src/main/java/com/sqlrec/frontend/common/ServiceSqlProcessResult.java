package com.sqlrec.frontend.common;

import com.sqlrec.model.ServiceManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;

public class ServiceSqlProcessResult extends SqlProcessResult {
    private String serviceName;
    private long lastCheckTime = 0;
    private boolean cachedCompleted = false;

    public ServiceSqlProcessResult() {
        super();
    }

    public ServiceSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            THandleIdentifier handleIdentifier,
            String queryId,
            String msg,
            String serviceName
    ) {
        super(enumerable, fields, handleIdentifier, queryId, msg);
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public boolean isCompleted() {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCheckTime < 10000) {
                return cachedCompleted;
            }
            lastCheckTime = currentTime;

            boolean completed = ServiceManager.isServiceOperationCompleted(serviceName);
            cachedCompleted = completed;
            return completed;
        } catch (Exception e) {
            setException(e);
        }
        return false;
    }
}
