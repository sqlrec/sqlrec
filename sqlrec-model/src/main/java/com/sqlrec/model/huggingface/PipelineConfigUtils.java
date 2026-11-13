package com.sqlrec.model.huggingface;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;
import com.sqlrec.model.common.ModelConfigUtils;

import java.util.List;
import java.util.Map;

/** Builds the JSON consumed by the snapshot download job. */
public final class PipelineConfigUtils {
    private static final List<String> SERVICE_ONLY_PARAMS = List.of(
            "served_model_name", "openai_api_key_secret", "openai_api_key_secret_key",
            "openai_max_request_bytes", "openai_max_batch_inputs");
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
        return buildTrainConfig(model, trainConf, operationParams(model, trainConf.getParams()));
    }

    /** Builds training JSON from merged, validated operation parameters. */
    static String buildTrainConfig(ModelConf model, ModelTrainConf trainConf, Map<String, String> params) {
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
        params.forEach((key, value) -> {
            if (!SERVICE_ONLY_PARAMS.contains(key)) modelParams.addProperty(key, value);
        });
        config.add("model_params", modelParams);
        return ModelConfigUtils.toPrettyJson(config);
    }

    public static String generateServiceConfig(ModelConf model, Map<String, String> serviceParams) {
        return buildServiceConfig(model, operationParams(model, serviceParams));
    }

    /** Builds serving JSON from merged, validated operation parameters. */
    static String buildServiceConfig(ModelConf model, Map<String, String> params) {
        String alias = params.getOrDefault("served_model_name", model.getModelName());
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("served_model_name must not be blank");
        }
        if (Config.OPENAI_MAX_REQUEST_BYTES.getValue(params) <= 0
                || Config.OPENAI_MAX_BATCH_INPUTS.getValue(params) <= 0) {
            throw new IllegalArgumentException("OpenAI request limits must be positive");
        }
        String secret = Config.OPENAI_API_KEY_SECRET.getValueOrNull(params);
        if (secret != null && (secret.isBlank() || Config.OPENAI_API_KEY_SECRET_KEY.getValue(params).isBlank())) {
            throw new IllegalArgumentException("OpenAI Secret name and key must not be blank");
        }
        JsonObject config = new JsonObject();
        params.forEach(config::addProperty);
        config.addProperty("served_model_name", alias);
        return ModelConfigUtils.toPrettyJson(config);
    }

    private static void addCsv(JsonObject target, String key, String value) {
        JsonArray array = new JsonArray();
        for (String part : FieldTypeUtils.parseCsvList(value)) {
            array.add(part);
        }
        target.add(key, array);
    }
}
