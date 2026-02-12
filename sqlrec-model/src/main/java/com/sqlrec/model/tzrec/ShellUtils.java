package com.sqlrec.model.tzrec;

import com.sqlrec.model.common.Model;
import com.sqlrec.model.common.ModelTrainConf;

public class ShellUtils {
    public static String genTrainModelShell(Model model, ModelTrainConf trainConf) {
        String shell = "#!/bin/bash\n" +
                "set -ex\n" +
                "\n" +
                "NODE_RANK=${JOB_COMPLETION_INDEX:-0}\n" +
                "MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}\n" +
                "\n" +
                "torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\n" +
                "    --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\n" +
                "    -m tzrec.train_eval \\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME;
        return shell;
    }
}
