package com.sqlrec.executor;

import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.model.ServiceManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class ServiceSqlProcessResult extends CachedCompletionSqlProcessResult {
    private String serviceName;

    public ServiceSqlProcessResult() {
        super();
    }

    public ServiceSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            String serviceName
    ) {
        super(enumerable, fields);
        this.serviceName = serviceName;
    }

    public static ServiceSqlProcessResult msg(String msg, String fieldName, String serviceName) {
        return new ServiceSqlProcessResult(
                DataTransformUtils.getMsgEnumerable(msg),
                DataTypeUtils.getStringTypeField(fieldName),
                serviceName
        );
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public boolean isCompleted() {
        return checkCompletion(() -> ServiceManager.isServiceOperationCompleted(serviceName));
    }
}
