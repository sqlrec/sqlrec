package com.sqlrec.model.gbdt;

import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

/**
 * Generates bash scripts for GBDT train, export, and serving.
 */
public class ShellUtils {

    /** Train and export share the same template; only the module action differs. */
    private static String genPythonModuleShell(String module) {
        return "#!/bin/bash\n" +
                "set -ex\n" +
                "export PYTHONPATH=/app:$PYTHONPATH\n" +
                "\n" +
                "exec python -m " + module + " \\\n" +
                "    --pipeline_config_path " + Config.SHELL_DIR + "/" + Config.PIPELINE_CONFIG_NAME;
    }

    private static String module(ModelType modelType, String action) {
        return "gbdt." + action + "_" + modelType.getKey();
    }

    public static String genTrainModelShell(ModelType modelType) {
        return genPythonModuleShell(module(modelType, "train"));
    }

    public static String genExportModelShell(ModelType modelType) {
        return genPythonModuleShell(module(modelType, "export"));
    }

    /**
     * Downloads model artifacts from remote storage via the Hadoop client, then
     * launches the C++ server via {@code exec} (PID 1 for direct K8s SIGTERM
     * handling).
     *
     * <p>The Hadoop client is mounted into the serving container by the framework
     * (with {@code HADOOP_HOME} pointing at it), so {@code hadoop fs -get} pulls
     * the model directory from HDFS directly. The cache dir is removed first so
     * the remote directory's contents land at the top level (matching the layout
     * the C++ servers expect: {@code model.cbm}/{@code model.onnx} and
     * {@code schema.json}).
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
                "\n" +
                "LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}\n" +
                "rm -rf \"$LOCAL_CACHE_DIR\"\n" +
                "${HADOOP_HOME}/bin/hadoop fs -get '" + escapedPath + "' \"$LOCAL_CACHE_DIR\"\n" +
                "\n" +
                "exec /app/" + serverBinary + " \"$LOCAL_CACHE_DIR\" 80\n";
    }
}
