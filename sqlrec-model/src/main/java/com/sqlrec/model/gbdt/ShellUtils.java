package com.sqlrec.model.gbdt;

import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

/**
 * Generates bash scripts for GBDT train, export, and serving.
 */
public class ShellUtils {

    public static String genTrainModelShell(ModelType modelType) {
        String module;
        if (modelType == ModelType.CATBOOST) {
            module = "gbdt.train_catboost";
        } else if (modelType == ModelType.XGBOOST) {
            module = "gbdt.train_xgboost";
        } else {
            module = "gbdt.train_lightgbm";
        }
        return "#!/bin/bash\n" +
                "set -ex\n" +
                "export PYTHONPATH=/app:$PYTHONPATH\n" +
                "\n" +
                "exec python -m " + module + " \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME;
    }

    public static String genExportModelShell(ModelType modelType) {
        String module;
        if (modelType == ModelType.CATBOOST) {
            module = "gbdt.export_catboost";
        } else if (modelType == ModelType.XGBOOST) {
            module = "gbdt.export_xgboost";
        } else {
            module = "gbdt.export_lightgbm";
        }
        return "#!/bin/bash\n" +
                "set -ex\n" +
                "export PYTHONPATH=/app:$PYTHONPATH\n" +
                "\n" +
                "exec python -m " + module + " \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME;
    }

    /**
     * Downloads model artifacts from remote storage via Python, then launches
     * the C++ server via {@code exec} (PID 1 for direct K8s SIGTERM handling).
     *
     * <p>CatBoost uses {@code catboost_server} (native C API). LightGBM and
     * XGBoost both use {@code onnx_server} (ONNX Runtime).
     */
    public static String genServeModelShell(ModelType modelType, String modelCheckpointDir) {
        String serverBinary = modelType == ModelType.CATBOOST
                ? "catboost_server"
                : "onnx_server";
        String escapedPath = modelCheckpointDir.replace("'", "'\\''");
        return "#!/bin/bash\n" +
                "set -ex\n" +
                "export PYTHONPATH=/app:$PYTHONPATH\n" +
                "\n" +
                "LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}\n" +
                "python -c \"import sys; from common.filesystem import download_dir; download_dir(sys.argv[1], sys.argv[2])\" '" + escapedPath + "' \"$LOCAL_CACHE_DIR\"\n" +
                "\n" +
                "exec /app/" + serverBinary + " $LOCAL_CACHE_DIR 80\n";
    }
}
