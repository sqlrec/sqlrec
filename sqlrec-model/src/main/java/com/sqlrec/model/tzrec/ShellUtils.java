package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.model.common.ShellScriptUtils;
import com.sqlrec.common.utils.JsonUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class ShellUtils {

    private static final Set<String> STRUCTURAL_OPTIONS = Set.of("embedding_dim", "num_buckets", "hidden_units",
            "user_features", "item_features", "user_hidden_units", "item_hidden_units", "output_dim",
            "num_expert", "expert_hidden_units", "task_hidden_units");

    /** Train and export share the torchrun prologue; only the module and extra args differ. */
    private static String torchrunShell(String module, String extraArgs) {
        return "#!/bin/bash\n" +
                "set -ex\n" +
                "\n" +
                "NODE_RANK=${JOB_COMPLETION_INDEX:-0}\n" +
                "MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}\n" +
                "\n" +
                "torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\\n" +
                "    --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\\n" +
                "    /app/run.py --mode " + module + " \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME +
                extraArgs;
    }

    public static String genTrainModelShell(ModelConf model, ModelTrainConf trainConf) {
        return torchrunShell("train", checkpointStructureArgs(model, trainConf.getParams()));
    }

    private static String checkpointStructureArgs(ModelConf model, Map<String, String> params) {
        if (params == null) return "";
        Map<String, String> options = new java.util.TreeMap<>();
        params.keySet().stream().filter(key -> STRUCTURAL_OPTIONS.contains(key)
                || key.startsWith("column.") || key.startsWith("task."))
                .forEach(key -> options.put(key, params.get(key)));
        if (options.isEmpty()) return "";
        // Per-column options take precedence over a global feature default.
        ModelConf effective = PipelineConfigUtils.effectiveModel(model, params);
        Map<String, Object> maskedFeatures = new LinkedHashMap<>();
        for (String key : Set.of("embedding_dim", "num_buckets")) {
            if (!options.containsKey(key)) continue;
            String suffix = key.equals("embedding_dim") ? ".embedding_dim" : ".bucket_size";
            maskedFeatures.put(key, FeatureOptions.features(effective).stream()
                    .map(field -> field.getName())
                    .filter(name -> effective.getParams().containsKey("column." + name + suffix)).toList());
        }
        String metadata = JsonUtils.getGson().toJson(Map.of("options", options, "masked_features", maskedFeatures));
        return " \\\n    --checkpoint_structure_options " + ShellScriptUtils.quote(metadata);
    }

    public static String genExportModelShell(ModelConf model, ModelExportConf exportConf, String exportDir) {
        // Single-quote and escape the directory so shell metacharacters in exportDir (which derives
        // from user-configured base_model_dir) cannot break out of the argument and inject commands.
        return torchrunShell(
                "export",
                " \\\n    --export_dir " + ShellScriptUtils.quote(exportDir) + checkpointStructureArgs(model, exportConf.getParams())
        );
    }
}
