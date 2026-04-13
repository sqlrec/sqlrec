package com.sqlrec.frontend.common;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.model.ModelControllerFactory;
import com.sqlrec.model.ModelEntityConverter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CommonUtils {
    
    public static void addModelInfo(List<List<String>> rows, Model model) throws Exception {
        ModelConfig modelConfig = ModelEntityConverter.convertToModel(model.getDdl());
        addModelInfo(rows, modelConfig, model);
    }
    
    public static void addModelInfo(List<List<String>> rows, String modelDdl, Model model) throws Exception {
        ModelConfig modelConfig = ModelEntityConverter.convertToModel(modelDdl);
        addModelInfo(rows, modelConfig, model);
    }
    
    public static void addModelInfo(List<List<String>> rows, ModelConfig modelConfig, Model model) throws Exception {
        ModelController modelController = ModelControllerFactory.getModelController(modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException("model controller not found for model: " + model.getName());
        }
        
        List<FieldSchema> inputFields = modelConfig.getInputFields();
        List<FieldSchema> outputFields = modelController.getOutputFields(modelConfig);
        Map<String, String> params = modelConfig.getParams();
        
        rows.add(java.util.Arrays.asList("# Model Information", ""));
        rows.add(java.util.Arrays.asList("Model Name:", model.getName()));
        rows.add(java.util.Arrays.asList("Created At:", formatTimestamp(model.getCreatedAt())));
        rows.add(java.util.Arrays.asList("Updated At:", formatTimestamp(model.getUpdatedAt())));
        rows.add(java.util.Arrays.asList("", ""));
        
        addFieldSection(rows, "# Input Fields", inputFields);
        addFieldSection(rows, "# Output Fields", outputFields);
        addParametersSection(rows, "# Model Parameters", params);
    }
    
    public static void addCheckpointInfo(List<List<String>> rows, Checkpoint checkpoint) {
        rows.add(java.util.Arrays.asList("", ""));
        rows.add(java.util.Arrays.asList("# Checkpoint Information", ""));
        rows.add(java.util.Arrays.asList("Checkpoint Name:", checkpoint.getCheckpointName()));
        rows.add(java.util.Arrays.asList("Checkpoint Type:", 
            checkpoint.getCheckpointType() != null ? checkpoint.getCheckpointType() : "N/A"));
        rows.add(java.util.Arrays.asList("Status:", 
            checkpoint.getStatus() != null ? checkpoint.getStatus() : "N/A"));
        rows.add(java.util.Arrays.asList("Created At:", formatTimestamp(checkpoint.getCreatedAt())));
        rows.add(java.util.Arrays.asList("Updated At:", formatTimestamp(checkpoint.getUpdatedAt())));
    }
    
    public static void addServiceInfo(List<List<String>> rows, Service service) throws Exception {
        rows.add(java.util.Arrays.asList("# Service Information", ""));
        rows.add(java.util.Arrays.asList("Service Name:", service.getName()));
        rows.add(java.util.Arrays.asList("Model Name:", service.getModelName()));
        if (service.getCheckpointName() != null) {
            rows.add(java.util.Arrays.asList("Checkpoint Name:", service.getCheckpointName()));
        }
        if (service.getUrl() != null) {
            rows.add(java.util.Arrays.asList("URL:", service.getUrl()));
        }
        rows.add(java.util.Arrays.asList("Created At:", formatTimestamp(service.getCreatedAt())));
        rows.add(java.util.Arrays.asList("Updated At:", formatTimestamp(service.getUpdatedAt())));
        rows.add(java.util.Arrays.asList("", ""));
        
        if (service.getModelDdl() != null) {
            ModelConfig modelConfig = ModelEntityConverter.convertToModel(service.getModelDdl());
            ModelController modelController = ModelControllerFactory.getModelController(modelConfig);
            if (modelController == null) {
                throw new IllegalArgumentException("model controller not found for model: " + service.getModelName());
            }
            
            List<FieldSchema> inputFields = modelConfig.getInputFields();
            List<FieldSchema> outputFields = modelController.getOutputFields(modelConfig);
            Map<String, String> params = modelConfig.getParams();
            
            addFieldSection(rows, "# Input Fields", inputFields);
            addFieldSection(rows, "# Output Fields", outputFields);
            addParametersSection(rows, "# Model Parameters", params);
        }
    }
    
    private static void addFieldSection(List<List<String>> rows, String sectionTitle, List<FieldSchema> fields) {
        rows.add(java.util.Arrays.asList(sectionTitle, ""));
        rows.add(java.util.Arrays.asList("Name", "Type"));
        if (fields != null && !fields.isEmpty()) {
            for (FieldSchema field : fields) {
                rows.add(java.util.Arrays.asList(field.getName(), field.getType()));
            }
        } else {
            rows.add(java.util.Arrays.asList("(none)", ""));
        }
        rows.add(java.util.Arrays.asList("", ""));
    }
    
    private static void addParametersSection(List<List<String>> rows, String sectionTitle, Map<String, String> params) {
        rows.add(java.util.Arrays.asList(sectionTitle, ""));
        rows.add(java.util.Arrays.asList("Key", "Value"));
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                rows.add(java.util.Arrays.asList(entry.getKey(), entry.getValue()));
            }
        } else {
            rows.add(java.util.Arrays.asList("(none)", ""));
        }
    }
    
    private static String formatTimestamp(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }
}
