package com.sqlrec.model;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ModelControllerFactory {
    private static final Logger log = LoggerFactory.getLogger(ModelControllerFactory.class);
    public static Map<String, ModelController> getModelControllerMap() {
        return ControllerHolder.CONTROLLERS;
    }

    public static ModelController getModelController(String modelName) {
        return getModelControllerMap().get(modelName);
    }

    public static ModelController getModelController(ModelConf modelConfig) {
        String modelAlgorithmName = ModelConfigs.MODEL.getValue(modelConfig.getParams());
        return ModelControllerFactory.getModelController(modelAlgorithmName);
    }

    static ModelController getRequiredModelController(ModelConf modelConfig) {
        ModelController modelController = getModelController(modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException(
                    "Model controller not found for model name: " + modelConfig.getModelName()
            );
        }
        return modelController;
    }

    private static final class ControllerHolder {
        private static final Map<String, ModelController> CONTROLLERS = loadControllers();

        private static Map<String, ModelController> loadControllers() {
            Map<String, ModelController> controllers = new HashMap<>();
            for (ModelController controller : ServiceLoader.load(ModelController.class)) {
                String modelName = controller.getModelName();
                if (controllers.containsKey(modelName)) {
                    log.warn("Model name {} is duplicated, existing implementation will be overridden", modelName);
                }
                controllers.put(modelName, controller);
            }
            return Collections.unmodifiableMap(controllers);
        }
    }
}
