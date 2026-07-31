package com.sqlrec.common.runtime;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;

import java.util.Map;

public interface ReadonlyContext {
    ServiceConf getServiceConfig(String serviceName);

    ModelController getModelController(ModelConf modelConfig);

    String getVariable(String key);

    Map<String, String> getVariables();

    Map<String, String> getMetricsTags();

    String getLogId();

    Object getTraceContext();
}
