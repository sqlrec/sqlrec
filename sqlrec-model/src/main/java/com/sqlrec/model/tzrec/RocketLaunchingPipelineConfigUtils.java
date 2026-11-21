package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import java.util.Map;

/** Builds native RocketLaunching configs without modifying the TZRec model factory. */
final class RocketLaunchingPipelineConfigUtils {
    private RocketLaunchingPipelineConfigUtils() {}

    static String train(ModelConf model, ModelTrainConf train) {

        return PipelineConfigUtils.generateTrainConfigPrefix(model, train).append(modelConfig(model, train.getParams())).toString();
    }

    static String export(ModelConf model, ModelExportConf export) {

        return PipelineConfigUtils.generateExportConfigPrefix(model, export).append(modelConfig(model, Map.of())).toString();
    }

    static String modelConfig(ModelConf model, Map<String, String> training) {
        StringBuilder config = new StringBuilder("model_config {\n");
        PipelineConfigUtils.addFeatureGroup(config, "deep", FeatureOptions.features(model).stream().map(FieldSchema::getName).toList(), "DEEP");
        config.append("    rocket_launching {\n");
        String shared = Config.SHARE_HIDDEN_UNITS.getValueOrNull(model.getParams());
        if (shared != null) addMlp(config, "share", shared);
        addMlp(config, "booster", Config.BOOSTER_HIDDEN_UNITS.getValue(model.getParams()));
        addMlp(config, "light", Config.LIGHT_HIDDEN_UNITS.getValue(model.getParams()));
        config.append("        feature_based_distillation: true\n")
                .append("        feature_distillation_function: COSINE\n    }\n")
                .append("    num_class: 1\n    losses { binary_cross_entropy {} }\n");
        MetricOptions.addMetrics(config, training, MetricOptions.BINARY, "    ");
        return config.append("}\n").toString();
    }

    private static void addMlp(StringBuilder config, String name, String widths) {
        config.append("        ").append(name).append("_mlp { hidden_units: [")
                .append(RocketLaunchingOptions.hiddenUnits(widths)).append("] }\n");
    }
}
