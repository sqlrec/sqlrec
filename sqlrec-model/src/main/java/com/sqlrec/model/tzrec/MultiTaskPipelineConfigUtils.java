package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

/** Adapts SQLRec's task options to TZRec's native MMoE configuration. */
final class MultiTaskPipelineConfigUtils {
    private MultiTaskPipelineConfigUtils() {}

    static String train(ModelConf model, ModelTrainConf train) {
        model = PipelineConfigUtils.effectiveModel(model, train.getParams());
        return PipelineConfigUtils.generateTrainConfigPrefix(model, train).append(modelConfig(model)).toString();
    }

    static String export(ModelConf model, ModelExportConf export) {
        model = PipelineConfigUtils.effectiveModel(model, export.getParams());
        return PipelineConfigUtils.generateExportConfigPrefix(model, export).append(modelConfig(model)).toString();
    }

    static String modelConfig(ModelConf model) {
        StringBuilder config = new StringBuilder("model_config {\n");
        PipelineConfigUtils.addFeatureGroup(config, "deep", FeatureOptions.features(model).stream().map(FieldSchema::getName).toList(), "DEEP");
        config.append("    mmoe {\n        num_expert: ").append(Config.NUM_EXPERT.getValue(model.getParams())).append("\n");
        config.append("        expert_mlp { hidden_units: [").append(MultiTaskOptions.hiddenUnits(Config.EXPERT_HIDDEN_UNITS.getValue(model.getParams()))).append("] }\n");
        for (MultiTaskOptions.TaskSpec task : MultiTaskOptions.parse(model)) {
            config.append("        task_towers {\n");
            config.append("            tower_name: ").append(PipelineConfigUtils.quoted(task.label())).append("\n");
            config.append("            label_name: ").append(PipelineConfigUtils.quoted(task.label())).append("\n");
            config.append("            num_class: 1\n            mlp { hidden_units: [").append(task.hiddenUnits()).append("] }\n");
            config.append("            weight: ").append(task.weight()).append("\n");
            config.append("            losses { ").append(task.type().equals("binary") ? "binary_cross_entropy" : "l2_loss").append(" {} }\n");
            for (String metric : task.metrics()) config.append("            metrics { ").append(metric).append(" {} }\n");
            config.append("        }\n");
        }
        return config.append("    }\n}\n").toString();
    }
}
