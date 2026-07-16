package com.sqlrec.model.tzrec;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.SqlRecConfigs;

/**
 * Configuration class for model-related parameters
 */
public class Config {
    public static final String SHELL_DIR = "/data";
    public static final String PIPELINE_CONFIG_NAME = "pipeline.config";
    public static final String START_SHELL_NAME = "start.sh";
    public static final String SERVICE_SHELL_PATH = "/app/server.sh";

    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
            "image",
            "sqlrec/tzrec", "Docker image", null, String.class);
    public static final ConfigOption<String> VERSION = new ConfigOption<>(
            "version",
            SqlRecConfigs.SQLREC_VERSION.getValue() + "-cpu", "Docker image version", null, String.class);

    // Training configuration
    public static final ConfigOption<Double> SPARSE_LR = new ConfigOption<>(
            "sparse_lr",
            0.001, "Learning rate for sparse features", null, Double.class);
    public static final ConfigOption<Double> DENSE_LR = new ConfigOption<>(
            "dense_lr",
            0.001, "Learning rate for dense features", null, Double.class);
    public static final ConfigOption<Integer> NUM_EPOCHS = new ConfigOption<>(
            "num_epochs",
            1, "Number of training epochs", null, Integer.class);

    // Data configuration
    public static final ConfigOption<Integer> BATCH_SIZE = new ConfigOption<>(
            "batch_size",
            8192, "Batch size for training", null, Integer.class);
    public static final ConfigOption<Integer> NUM_WORKERS = new ConfigOption<>(
            "num_workers",
            8, "Number of data loading workers", null, Integer.class);

    // Feature configuration
    public static final ConfigOption<Integer> EMBEDDING_DIM = new ConfigOption<>(
            "embedding_dim",
            16, "Embedding dimension for categorical features", null, Integer.class);
    public static final ConfigOption<Integer> NUM_BUCKETS = new ConfigOption<>(
            "num_buckets",
            1000000, "Number of buckets for int features", null, Integer.class);

    // Data configuration
    public static final ConfigOption<String> LABEL_COLUMNS = new ConfigOption<>(
            "label_columns",
            null, "Label columns", null, String.class);

    // Model configuration
    public static final ConfigOption<String> HIDDEN_UNITS = new ConfigOption<>(
            "hidden_units",
            "512,256,128", "Hidden units for deep network", null, String.class);

    // Distributed training configuration
    public static final ConfigOption<Integer> NNODES = new ConfigOption<>(
            "nnodes",
            1, "Number of nodes for distributed training", null, Integer.class);
    public static final ConfigOption<Integer> NPROC_PER_NODE = new ConfigOption<>(
            "nproc_per_node",
            1, "Number of processes per node for distributed training", null, Integer.class);
    public static final ConfigOption<Integer> MASTER_PORT = new ConfigOption<>(
            "master_port",
            29500, "Master port for distributed training", null, Integer.class);

    public static final ConfigOption<String> USE_FSSPEC = new ConfigOption<>(
            "USE_FSSPEC",
            "1", "Use fsspec", null, String.class);

    public static final ConfigOption<String> USE_SPAWN_MULTI_PROCESS = new ConfigOption<>(
            "USE_SPAWN_MULTI_PROCESS",
            "1", "Use spawn multi process", null, String.class);

    public static final ConfigOption<String> USE_FARM_HASH_TO_BUCKETIZE = new ConfigOption<>(
            "USE_FARM_HASH_TO_BUCKETIZE",
            "true", "Use farm hash to bucketize", null, String.class);

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

    public static final ConfigOption<String> USER_FEATURES = new ConfigOption<>(
            "user_features",
            null, "User feature names for DSSM model (comma separated)", null, String.class);

    public static final ConfigOption<String> ITEM_FEATURES = new ConfigOption<>(
            "item_features",
            null, "Item feature names for DSSM model (comma separated)", null, String.class);

    public static final ConfigOption<String> USER_HIDDEN_UNITS = new ConfigOption<>(
            "user_hidden_units",
            "512,256,128", "Hidden units for user tower in DSSM", null, String.class);

    public static final ConfigOption<String> ITEM_HIDDEN_UNITS = new ConfigOption<>(
            "item_hidden_units",
            "512,256,128", "Hidden units for item tower in DSSM", null, String.class);

    public static final ConfigOption<Integer> OUTPUT_DIM = new ConfigOption<>(
            "output_dim",
            64, "Output embedding dimension for DSSM", null, Integer.class);
}
