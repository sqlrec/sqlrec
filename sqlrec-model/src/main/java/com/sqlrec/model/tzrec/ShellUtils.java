package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;

public class ShellUtils {

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
                "    -m " + module + " \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME +
                extraArgs;
    }

    public static String genTrainModelShell(ModelConf model, ModelTrainConf trainConf) {
        return torchrunShell("tzrec.train_eval", "");
    }

    public static String genExportModelShell(ModelConf model, ModelExportConf exportConf, String exportDir) {
        // Single-quote and escape the directory so shell metacharacters in exportDir (which derives
        // from user-configured base_model_dir) cannot break out of the argument and inject commands.
        String escapedDir = exportDir.replace("'", "'\\''");
        return torchrunShell("tzrec.export", " \\\n    --export_dir '" + escapedDir + "'");
    }
}
