package com.sqlrec.model.huggingface;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Hugging Face Transformers snapshots served by SQLRec's Python runtime. */
public class HuggingFaceTransformersModel implements ModelController {
    @Override
    public String getModelName() {
        return "huggingface.transformers";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        String task = Config.TASK.getValue(model.getParams());
        return switch (task) {
            case "text-classification" -> Arrays.asList(
                    new FieldSchema("label", "STRING"), new FieldSchema("score", "FLOAT"));
            case "text-generation" -> Collections.singletonList(
                    new FieldSchema("generated_text", "STRING"));
            case "embedding", "image-embedding" -> Collections.singletonList(
                    new FieldSchema("embedding", "ARRAY<FLOAT>"));
            default -> throw new IllegalArgumentException("Unsupported Hugging Face task: " + task);
        };
    }

    @Override
    public String checkModel(ModelConf model) {
        Map<String, String> params = model.getParams();
        if (params == null) {
            return "model parameters are required for huggingface.transformers";
        }
        if (StringUtils.isBlank(Config.REPO_ID.getValueOrNull(params))) {
            return "repo_id is required for huggingface.transformers";
        }
        String task;
        try {
            task = Config.TASK.getValue(params);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
        String requiredColumn = switch (task) {
            case "text-classification", "embedding" -> Config.TEXT_COLUMN.getValueOrNull(params);
            case "text-generation" -> Config.PROMPT_COLUMN.getValueOrNull(params);
            case "image-embedding" -> Config.IMAGE_COLUMN.getValueOrNull(params);
            default -> null;
        };
        if (StringUtils.isBlank(requiredColumn)) {
            return switch (task) {
                case "text-classification", "embedding" -> "text_column is required for task " + task;
                case "text-generation" -> "prompt_column is required for task text-generation";
                case "image-embedding" -> "image_column is required for task image-embedding";
                default -> "unsupported task: " + task;
            };
        }
        String error = validateStringField(model, requiredColumn);
        if (error != null) {
            return error;
        }
        String pairColumn = Config.TEXT_PAIR_COLUMN.getValueOrNull(params);
        if (StringUtils.isNotBlank(pairColumn)) {
            if (!"text-classification".equals(task)) {
                return "text_pair_column is only supported for text-classification";
            }
            return validateStringField(model, pairColumn);
        }
        return null;
    }

    private String validateStringField(ModelConf model, String name) {
        if (model.getInputFields() == null) {
            return "input field '" + name + "' is not defined";
        }
        for (FieldSchema field : model.getInputFields()) {
            if (field.getName().equalsIgnoreCase(name)) {
                return "STRING".equalsIgnoreCase(field.getType())
                        ? null : "input field '" + name + "' must be STRING";
            }
        }
        return "input field '" + name + "' is not defined";
    }

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        Map<String, String> params = PipelineConfigUtils.mergeParams(model.getParams(), trainConf.getParams());
        String pipelineConfig = PipelineConfigUtils.generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainShell(model.getPath(), trainConf.getModelDir(), trainConf.getId());
        return HuggingFaceK8sYamlUtils.genTrainJobYaml(pipelineConfig, shell, trainConf.getId(), params);
    }

    @Override
    public boolean requiresTrainingData() {
        return false;
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        throw new UnsupportedOperationException("huggingface.transformers does not support model export; origin checkpoints can be served directly");
    }

    @Override
    public String getExportCleanPath(ModelExportConf exportConf) {
        throw new UnsupportedOperationException("huggingface.transformers does not support model export; origin checkpoints can be served directly");
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        throw new UnsupportedOperationException("huggingface.transformers does not support model export; origin checkpoints can be served directly");
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        return HuggingFaceK8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        return HuggingFaceK8sYamlUtils.getServiceK8sYaml(model, serviceConf);
    }

    @Override
    public String validateServiceCheckpointType(String checkpointType) {
        return Consts.CHECKPOINT_TYPE_ORIGIN.equals(checkpointType)
                ? null : "huggingface.transformers requires an origin checkpoint";
    }
}
