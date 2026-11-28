package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

/** Multi-target ranking with one service and an ordered output for every task. */
public class MMoEModel extends TzrecModelBase {
    @Override
    protected String getModelNameSuffix() {
        return "mmoe";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return MultiTaskOptions.parse(model).stream().map(task -> new FieldSchema(task.outputName(), "FLOAT")).toList();
    }

    @Override
    public String checkModel(ModelConf model) {
        return FeatureOptions.validate(model, FeatureOptions.ModelType.MMOE);
    }

    @Override
    protected String generateModelConfig(ModelConf model, Map<String, String> trainingOptions) {
        StringBuilder config = new StringBuilder("model_config {\n");
        PipelineConfigUtils.addDeepFeatureGroup(config, model);
        Integer expertCount = Config.NUM_EXPERT.getValue(model.getParams());
        String hiddenUnits = MultiTaskOptions.hiddenUnits(Config.EXPERT_HIDDEN_UNITS.getValue(model.getParams()));
        config.append("    mmoe {\n        num_expert: ").append(expertCount).append("\n")
                .append("        expert_mlp { hidden_units: [").append(hiddenUnits).append("] }\n");
        for (MultiTaskOptions.TaskSpec task : MultiTaskOptions.parse(model, trainingOptions)) {
            String label = PipelineConfigUtils.quoted(task.label());
            String loss = task.type().equals("binary") ? "binary_cross_entropy" : "l2_loss";
            config.append("        task_towers {\n")
                    .append("            tower_name: ").append(label).append("\n")
                    .append("            label_name: ").append(label).append("\n")
                    .append("            num_class: 1\n")
                    .append("            mlp { hidden_units: [").append(task.hiddenUnits()).append("] }\n")
                    .append("            weight: ").append(task.weight()).append("\n")
                    .append("            losses { ").append(loss).append(" {} }\n");
            MetricOptions.addMetrics(config, trainingOptions, task.metrics(), "            ");
            config.append("        }\n");
        }
        return config.append("    }\n}\n").toString();
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf export) {
        return List.of(export.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf export) {
        return null;
    }
}
