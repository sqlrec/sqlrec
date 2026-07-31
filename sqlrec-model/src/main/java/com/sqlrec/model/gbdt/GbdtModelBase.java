package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared {@link ModelController} implementation for GBDT models (CatBoost / LightGBM / XGBoost).
 *
 * <p>Concrete subclasses only need to provide a model name (e.g. {@code gbdt.lightgbm},
 * {@code gbdt.xgboost}, {@code gbdt.catboost}) and the corresponding {@link ModelType}. All K8s YAML / pipeline
 * config / shell generation logic is identical across GBDT backends and lives here.
 *
 * <p>GBDT models output a single {@code probs} (FLOAT) prediction column for binary / regression
 * objectives. Multi-class objectives are not exposed in the default output schema.
 */
public abstract class GbdtModelBase implements ModelController {

    private final ModelType modelType;

    protected GbdtModelBase(ModelType modelType) {
        this.modelType = modelType;
    }

    protected abstract String getModelNameSuffix();

    @Override
    public final String getModelName() {
        return "gbdt." + getModelNameSuffix();
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return Collections.singletonList(new FieldSchema("probs", "FLOAT"));
    }

    @Override
    public String checkModel(ModelConf model) {
        Map<String, String> params = model.getParams();
        if (params == null || params.get(Config.LABEL_COLUMNS.getKey()) == null
                || params.get(Config.LABEL_COLUMNS.getKey()).isEmpty()) {
            return "label_columns is required for GBDT model";
        }
        if (model.getInputFields() == null || model.getInputFields().isEmpty()) {
            return "input_fields is required for GBDT model";
        }

        // label_columns may be comma-separated (e.g. "label1,label2").
        String labelCol = params.get(Config.LABEL_COLUMNS.getKey());
        java.util.Set<String> labelNames = Arrays.stream(labelCol.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        // Validate that label columns exist in input fields.
        java.util.Set<String> inputFieldNames = new java.util.HashSet<>();
        for (FieldSchema f : model.getInputFields()) {
            inputFieldNames.add(f.getName());
        }
        for (String label : labelNames) {
            if (!inputFieldNames.contains(label)) {
                return "Label column '" + label + "' not found in input_fields";
            }
        }

        // Validate feature field types.
        List<String> unsupported = new ArrayList<>();
        for (FieldSchema f : model.getInputFields()) {
            if (labelNames.contains(f.getName())) {
                continue;
            }
            String error = validateFeatureType(f.getName(), f.getType());
            if (error != null) {
                unsupported.add(error);
            }
        }
        if (!unsupported.isEmpty()) {
            return String.join("; ", unsupported);
        }
        return null;
    }

    /**
     * Validate that a feature type is supported by this model.
     * Returns an error message if unsupported, or {@code null} if valid.
     *
     * <ul>
     *   <li>CatBoost: supports int, bigint, string, float, double; array types are unsupported.</li>
     *   <li>LightGBM/XGBoost: supports float, double only; int, bigint, string, and array types are unsupported.</li>
     * </ul>
     */
    private String validateFeatureType(String name, String type) {
        if (type == null) {
            return "Feature '" + name + "' has null type";
        }
        String lower = type.toLowerCase();

        // Array types are unsupported by both frameworks.
        if (lower.startsWith("array<")) {
            return getModelName() + " does not support array-type features, but '" + name + "' is " + type;
        }

        if (modelType == ModelType.LIGHTGBM || modelType == ModelType.XGBOOST) {
            if (!lower.equals("float") && !lower.equals("double")) {
                return getModelName() + " only supports float/double features, but '" + name + "' is " + type
                        + ". Consider using CatBoost for categorical/integer features.";
            }
        } else {
            // CatBoost: int, bigint, string, float, double are valid.
            if (!lower.equals("int") && !lower.equals("bigint")
                    && !lower.equals("string") && !lower.equals("float")
                    && !lower.equals("double")) {
                return "CatBoost only supports int/bigint/string/float/double features, but '"
                        + name + "' is " + type;
            }
        }
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateTrainConfig(modelType, model, trainConf);
        String shell = ShellUtils.genTrainModelShell(modelType);
        Map<String, String> mergedParams = PipelineConfigUtils.mergeParams(
                model.getParams(), trainConf.getParams());
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), mergedParams);
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        String name = exportConf.getCheckpointName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("checkpoint_name is required for GBDT export");
        }
        return Collections.singletonList(name + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf exportConf) {
        return null;
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        String pipelineConfig = PipelineConfigUtils.generateExportConfig(exportConf);
        String shell = ShellUtils.genExportModelShell(modelType);
        Map<String, String> mergedParams = PipelineConfigUtils.mergeParams(
                model.getParams(), exportConf.getParams());
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), mergedParams);
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        return K8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        return K8sYamlUtils.getServiceK8sYaml(modelType, serviceConf);
    }
}
