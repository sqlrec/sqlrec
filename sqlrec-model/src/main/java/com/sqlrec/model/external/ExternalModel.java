package com.sqlrec.model.external;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ExternalModel implements ModelController {
    @Override
    public String getModelName() {
        return "external";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConfig model) {
        List<FieldSchema> outputFields = new ArrayList<>();
        String outputFieldsStr = Config.OUTPUT_COLUMNS.getValue(model.getParams());
        if (StringUtils.isNotEmpty(outputFieldsStr)) {
            String[] fieldParts = outputFieldsStr.split(",");
            for (String fieldPart : fieldParts) {
                String[] nameType = fieldPart.split(":");
                if (nameType.length == 2) {
                    outputFields.add(new FieldSchema(nameType[0].trim(), nameType[1].trim()));
                }
            }
        }
        return outputFields;
    }

    @Override
    public String checkModel(ModelConfig model) {
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        throw new UnsupportedOperationException("External model does not support training");
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        throw new UnsupportedOperationException("External model does not support export");
    }

    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        throw new UnsupportedOperationException("External model does not support export");
    }

    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        return Config.URL.getValue(serviceConf.getParams());
    }

    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        return "";
    }
}
