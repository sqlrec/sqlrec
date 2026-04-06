package com.sqlrec.common.runtime;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;

public interface ExecuteContext {
    String getVariable(String key);

    void setVariable(String key, String value);

    ServiceConfig getServiceConfig(String serviceName);

    ModelController getModelController(ModelConfig modelConfig);
}
