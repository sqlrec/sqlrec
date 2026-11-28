package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

/** Native booster/light training with a single light prediction service. */
public class RocketLaunchingModel extends TzrecModelBase {
    @Override
    protected String getModelNameSuffix() { return "rocket_launching"; }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return List.of(new FieldSchema("probs_light", "FLOAT"));
    }

    @Override
    public String checkModel(ModelConf model) {
        String error = FeatureOptions.validate(model, FeatureOptions.ModelType.ROCKET_LAUNCHING);
        if (error != null) return error;
        try {
            RocketLaunchingOptions.validate(model);
            return null;
        } catch (IllegalArgumentException e) {
            return "Invalid RocketLaunching configuration: " + e.getMessage();
        }
    }

    @Override
    protected String generateModelConfig(ModelConf model, Map<String, String> trainingOptions) {
        StringBuilder config = new StringBuilder("model_config {\n");
        PipelineConfigUtils.addDeepFeatureGroup(config, model);
        config.append("    rocket_launching {\n");
        String shared = Config.SHARE_HIDDEN_UNITS.getValueOrNull(model.getParams());
        if (shared != null) {
            addMlp(config, "share", shared);
        }
        addMlp(config, "booster", Config.BOOSTER_HIDDEN_UNITS.getValue(model.getParams()));
        addMlp(config, "light", Config.LIGHT_HIDDEN_UNITS.getValue(model.getParams()));
        config.append("        feature_based_distillation: true\n")
                .append("        feature_distillation_function: COSINE\n    }\n")
                .append("    num_class: 1\n    losses { binary_cross_entropy {} }\n");
        MetricOptions.addMetrics(config, trainingOptions, MetricOptions.BINARY, "    ");
        return config.append("}\n").toString();
    }

    private static void addMlp(StringBuilder config, String name, String widths) {
        config.append("        ").append(name).append("_mlp { hidden_units: [")
                .append(RocketLaunchingOptions.hiddenUnits(widths)).append("] }\n");
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf export) {
        return List.of(export.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf export) { return null; }
}
