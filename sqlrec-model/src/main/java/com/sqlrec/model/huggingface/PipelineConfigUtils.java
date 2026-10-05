package com.sqlrec.model.huggingface;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.ModelConfigUtils;

import java.util.List;
import java.util.Map;

/** Builds the JSON consumed by the snapshot download job. */
public final class PipelineConfigUtils {
    private PipelineConfigUtils() {
    }

    public static Map<String, String> mergeParams(Map<String, String> base, Map<String, String> overrides) {
        return ModelConfigUtils.mergeParams(base, overrides);
    }

    /** Operation overrides must preserve the declared SQL input/output contract. */
    static Map<String, String> operationParams(ModelConf model, Map<String, String> overrides) {
        Map<String, String> params = mergeParams(model.getParams(), overrides);
        if (!Config.TASK.getValue(model.getParams()).equals(Config.TASK.getValue(params))) {
            throw new IllegalArgumentException("Training/serving cannot change the task; create a new model instead");
        }
        if (params.containsKey("model") && !"huggingface.transformers".equals(params.get("model"))) {
            throw new IllegalArgumentException("Training/serving cannot change the model type; create a new model instead");
        }
        ModelConf effective = new ModelConf();
        effective.setInputFields(model.getInputFields());
        effective.setParams(params);
        String error = new HuggingFaceTransformersModel().checkModel(effective);
        if (error != null) throw new IllegalArgumentException(error);
        for (String key : List.of("text_column", "text_pair_column", "prompt_column", "image_column")) {
            String column = params.get(key);
            if (column == null || column.isBlank()) continue;
            for (FieldSchema field : model.getInputFields()) {
                if (field.getName().equalsIgnoreCase(column)) {
                    params.put(key, field.getName());
                    break;
                }
            }
        }
        return params;
    }

    public static String generateTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        Map<String, String> params = operationParams(model, trainConf.getParams());
        JsonObject config = new JsonObject();
        config.addProperty("framework", "huggingface.transformers");
        config.addProperty("model_name", model.getModelName());
        config.addProperty("checkpoint_name", trainConf.getCheckpointName());
        config.addProperty("repo_id", Config.REPO_ID.getValue(params));
        config.addProperty("revision", Config.REVISION.getValue(params));
        config.addProperty("task", Config.TASK.getValue(params));
        config.addProperty("trust_remote_code", Config.TRUST_REMOTE_CODE.getValue(params));
        addCsv(config, "allow_patterns", Config.ALLOW_PATTERNS.getValueOrNull(params));
        addCsv(config, "ignore_patterns", Config.IGNORE_PATTERNS.getValueOrNull(params));
        config.addProperty("force_download", Config.FORCE_DOWNLOAD.getValue(params));

        JsonArray inputFields = new JsonArray();
        if (model.getInputFields() != null) {
            for (FieldSchema field : model.getInputFields()) {
                JsonObject item = new JsonObject();
                item.addProperty("name", field.getName());
                item.addProperty("type", field.getType());
                inputFields.add(item);
            }
        }
        config.add("input_fields", inputFields);

        JsonObject modelParams = new JsonObject();
        params.forEach(modelParams::addProperty);
        config.add("model_params", modelParams);
        return ModelConfigUtils.toPrettyJson(config);
    }

    public static String generateServiceConfig(ModelConf model, Map<String, String> serviceParams) {
        Map<String, String> params = operationParams(model, serviceParams);
        JsonObject config = new JsonObject();
        params.forEach(config::addProperty);
        return ModelConfigUtils.toPrettyJson(config);
    }

    private static void addCsv(JsonObject target, String key, String value) {
        JsonArray array = new JsonArray();
        if (value != null) {
            for (String part : value.split(",")) {
                if (!part.trim().isEmpty()) {
                    array.add(part.trim());
                }
            }
        }
        target.add(key, array);
    }
}
