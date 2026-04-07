package com.sqlrec.runtime;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.runtime.ConfigContext;
import com.sqlrec.model.ModelControllerFactory;
import com.sqlrec.model.ServiceManager;

public class ConfigContextImpl implements ConfigContext {
    @Override
    public ServiceConfig getServiceConfig(String serviceName) {
        return ServiceManager.getServiceConfig(serviceName);
    }

    @Override
    public ModelController getModelController(ModelConfig modelConfig) {
        return ModelControllerFactory.getModelController(modelConfig);
    }
}
