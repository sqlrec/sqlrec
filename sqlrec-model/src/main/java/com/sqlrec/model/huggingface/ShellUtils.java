package com.sqlrec.model.huggingface;

/** Generates scripts for snapshot acquisition and serving. */
public final class ShellUtils {
    private ShellUtils() {
    }

    public static String genTrainShell(String modelBaseDir, String modelDir, String id) {
        String base = shellQuote(modelBaseDir);
        String target = shellQuote(modelDir);
        String staging = shellQuote(modelDir + ".__uploading__." + id);
        return "#!/bin/bash\n" +
                "set -euo pipefail\n" +
                "export PYTHONPATH=/app:${PYTHONPATH:-}\n" +
                "LOCAL_MODEL_DIR=${LOCAL_MODEL_DIR:-/tmp/huggingface_model}\n" +
                "rm -rf \"$LOCAL_MODEL_DIR\"\n" +
                "python -m huggingface.download --pipeline_config_path /data/pipeline.config --output_dir \"$LOCAL_MODEL_DIR\"\n" +
                "HADOOP=\"${HADOOP_HOME}/bin/hadoop\"\n" +
                "MODEL_BASE_DIR=" + base + "\n" +
                "MODEL_DIR=" + target + "\n" +
                "STAGING_DIR=" + staging + "\n" +
                "\"$HADOOP\" fs -mkdir -p \"$MODEL_BASE_DIR\"\n" +
                "\"$HADOOP\" fs -rm -r -f \"$STAGING_DIR\"\n" +
                "\"$HADOOP\" fs -put -f \"$LOCAL_MODEL_DIR\" \"$STAGING_DIR\"\n" +
                "\"$HADOOP\" fs -touchz \"$STAGING_DIR/_SUCCESS\"\n" +
                "\"$HADOOP\" fs -mv \"$STAGING_DIR\" \"$MODEL_DIR\"\n";
    }

    public static String genServeShell(String modelCheckpointDir) {
        return "#!/bin/bash\n" +
                "set -euo pipefail\n" +
                "export PYTHONPATH=/app:${PYTHONPATH:-}\n" +
                "LOCAL_MODEL_DIR=${LOCAL_MODEL_DIR:-/tmp/huggingface_model}\n" +
                "rm -rf \"$LOCAL_MODEL_DIR\"\n" +
                "mkdir -p \"$LOCAL_MODEL_DIR\"\n" +
                "${HADOOP_HOME}/bin/hadoop fs -get " + shellQuote(modelCheckpointDir + "/*") + " \"$LOCAL_MODEL_DIR\"/\n" +
                "test -f \"$LOCAL_MODEL_DIR/_SUCCESS\"\n" +
                "exec python -m huggingface.server --model_dir \"$LOCAL_MODEL_DIR\" --config /data/service.config --port 80\n";
    }

    static String shellQuote(String value) {
        if (value == null) {
            throw new IllegalArgumentException("shell value must not be null");
        }
        return "'" + value.replace("'", "'\\''") + "'";
    }
}
