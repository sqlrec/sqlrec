package com.sqlrec.model;

import com.sqlrec.model.common.ModelController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class ModelControllerFactory {
    private static final Logger log = LoggerFactory.getLogger(ModelControllerFactory.class);
    private static Map<String, ModelController> modelControllerMap;

    public static synchronized Map<String, ModelController> getModelControllerMap() {
        if (modelControllerMap == null) {
            modelControllerMap = new ConcurrentHashMap<>();
            ServiceLoader<ModelController> serviceLoader = ServiceLoader.load(ModelController.class);
            for (ModelController modelController : serviceLoader) {
                String modelName = modelController.getModelName();
                if (modelControllerMap.containsKey(modelName)) {
                    log.warn("Model name {} is duplicated, existing implementation will be overridden", modelName);
                }
                modelControllerMap.put(modelName, modelController);
            }
        }
        return modelControllerMap;
    }

    public static ModelController getModelController(String modelName) {
        if (modelControllerMap == null) {
            getModelControllerMap();
        }
        return modelControllerMap.getOrDefault(modelName, null);
    }
}
