package com.sqlrec.model.common;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.SqlRecConfigs;

/**
 * Configuration options shared by every model backend (gbdt / tzrec / external).
 *
 * <p>Holds the options that are byte-for-byte identical across {@code com.sqlrec.model.gbdt.Config}
 * and {@code com.sqlrec.model.tzrec.Config}: Docker image version, label columns, pod resource
 * requests/limits, replica count and the shell/pipeline-config file names. Backends only need to
 * declare their own {@code IMAGE} (image name differs) and backend-specific training params.
 *
 * <p>Backends inherit these constants via {@code extends ModelConfigBase}, so existing call sites
 * such as {@code Config.POD_CPU_CORES} keep resolving unchanged.
 */
public class ModelConfigBase {

    /** Mount path of the ConfigMap that carries the pipeline config + start shell. */
    public static final String SHELL_DIR = "/data";
    /** File name of the pipeline config inside the ConfigMap. */
    public static final String PIPELINE_CONFIG_NAME = "pipeline.config";
    /** File name of the start shell inside the ConfigMap. */
    public static final String START_SHELL_NAME = "start.sh";

    /**
     * Docker image version. Always derived from the platform version so that re-deploying the
     * platform automatically rolls model images forward.
     */
    public static final ConfigOption<String> VERSION = new ConfigOption<>(
            "version",
            SqlRecConfigs.SQLREC_VERSION.getValue() + "-cpu", "Docker image version", null, String.class);

    /** Comma-separated label column names. */
    public static final ConfigOption<String> LABEL_COLUMNS = new ConfigOption<>(
            "label_columns",
            null, "Label columns", null, String.class);

    // ---- Pod resource requests / limits (identical across backends) ----

    public static final ConfigOption<Integer> POD_CPU_CORES = new ConfigOption<>(
            "pod_cpu_cores",
            1, "Number of CPU cores for pod (used as resource request)", null, Integer.class);

    public static final ConfigOption<String> POD_MEMORY = new ConfigOption<>(
            "pod_memory",
            "2Gi", "Memory for pod (used as resource request)", null, String.class);

    public static final ConfigOption<String> POD_CPU_LIMIT = new ConfigOption<>(
            "pod_cpu_limit",
            null, "CPU limit for pod (e.g. '2' or '2000m'). If not set, no CPU limit is configured.", null, String.class);

    public static final ConfigOption<String> POD_MEMORY_LIMIT = new ConfigOption<>(
            "pod_memory_limit",
            null, "Memory limit for pod (e.g. '8Gi'). If not set, no memory limit is configured.", null, String.class);

    public static final ConfigOption<Integer> REPLICAS = new ConfigOption<>(
            "replicas",
            1, "Number of replicas for deployment", null, Integer.class);

    protected ModelConfigBase() {
    }
}
