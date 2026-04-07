package com.sqlrec.common.runtime;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;

public interface ConfigContext {
    ServiceConfig getServiceConfig(String serviceName);

    ModelController getModelController(ModelConfig modelConfig);
}
