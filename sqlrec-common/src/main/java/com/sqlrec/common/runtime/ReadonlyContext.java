package com.sqlrec.common.runtime;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;

import java.util.Map;

public interface ReadonlyContext {
    ServiceConfig getServiceConfig(String serviceName);

    ModelController getModelController(ModelConfig modelConfig);

    String getVariable(String key);

    Map<String, String> getVariables();

    Map<String, String> getMetricsTags();

    String getLogId();

    Object getTraceContext();
}
