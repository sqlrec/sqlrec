package com.sqlrec.model.huggingface;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.LinkedHashMap;
import java.util.Map;

/** Builds the JSON consumed by the snapshot download job. */
public final class PipelineConfigUtils {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

    private PipelineConfigUtils() {
    }

    public static Map<String, String> mergeParams(Map<String, String> base, Map<String, String> overrides) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        if (overrides != null) {
            merged.putAll(overrides);
        }
        return merged;
    }

    public static String generateTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        Map<String, String> params = mergeParams(model.getParams(), trainConf.getParams());
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
        if (model.getParams() != null) {
            model.getParams().forEach(modelParams::addProperty);
        }
        config.add("model_params", modelParams);
        return GSON.toJson(config) + "\n";
    }

    public static String generateServiceConfig(ModelConf model, Map<String, String> serviceParams) {
        Map<String, String> params = mergeParams(model.getParams(), serviceParams);
        JsonObject config = new JsonObject();
        params.forEach(config::addProperty);
        return GSON.toJson(config) + "\n";
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
