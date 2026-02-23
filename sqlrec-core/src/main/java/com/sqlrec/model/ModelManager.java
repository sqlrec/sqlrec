package com.sqlrec.model;

import com.sqlrec.model.common.Config;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelController;
import com.sqlrec.sql.parser.SqlCreateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelManager {
    private static final Logger log = LoggerFactory.getLogger(ModelManager.class);

    public static ModelConfig getAndCheckModel(SqlCreateModel sqlCreateModel) {
        try {
            ModelConfig model = ModelEntityConverter.convertToModel(sqlCreateModel);
            String modelName = Config.MODEL.getValue(model.params);
            ModelController modelController = ModelControllerFactory.getModelController(modelName);
            if (modelController == null) {
                throw new IllegalArgumentException("Model controller not found for model name: " + modelName);
            }
            String errorMessage = modelController.checkModel(model);
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }
            return model;
        } catch (Exception e) {
            throw new RuntimeException("Error while checking model: " + e.getMessage(), e);
        }
    }
}
