package com.sqlrec.model.tzrec;

import com.sqlrec.model.common.Model;
import com.sqlrec.model.common.ModelTrainConf;

public class ShellUtils {
    public static String genTrainModelShell(Model model, ModelTrainConf trainConf) {
        String shell = "torchrun --master_addr=localhost --master_port=32555 \\\n" +
                "    --nnodes=1 --nproc-per-node=1 --node_rank=0 \\\n" +
                "    -m tzrec.train_eval \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME;
        return shell;
    }
}
