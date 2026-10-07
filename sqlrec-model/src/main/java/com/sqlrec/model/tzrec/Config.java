package com.sqlrec.model.tzrec;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.model.common.ModelConfigBase;

/**
 * Configuration class for TZRec model-related parameters.
 *
 * <p>Inherits the shared options (image version, label columns, pod resources, replicas and the
 * shell/pipeline-config file names) from {@link ModelConfigBase}. Only TZRec-specific training
 * parameters and the TZRec image name are declared here.
 */
public class Config extends ModelConfigBase {

    /** Path of the serving entry shell baked into the tzrec image. */
    public static final String SERVICE_SHELL_PATH = "/app/server.sh";

    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
            "image",
            "sqlrec/tzrec", "Docker image", null, String.class);

    // Training configuration
    public static final ConfigOption<Integer> RANDOM_SEED = new ConfigOption<>(
            "random_seed", null, "Nonnegative Torch and NumPy training seed", null, Integer.class);
    public static final ConfigOption<Double> SPARSE_LR = new ConfigOption<>(
            "sparse_lr",
            0.001, "Learning rate for sparse features", null, Double.class);
    public static final ConfigOption<Double> DENSE_LR = new ConfigOption<>(
            "dense_lr",
            0.001, "Learning rate for dense features", null, Double.class);
    public static final ConfigOption<Integer> NUM_EPOCHS = new ConfigOption<>(
            "num_epochs",
            1, "Number of training epochs", null, Integer.class);
    public static final ConfigOption<String> MIXED_PRECISION = new ConfigOption<>(
            "mixed_precision",
            null, "Mixed precision training mode, e.g. BF16 or FP16, disabled by default",
            null, String.class);

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

    // Model configuration
    public static final ConfigOption<String> BOOSTER_HIDDEN_UNITS = new ConfigOption<>(
            "booster_hidden_units", "256,128,64", "RocketLaunching booster hidden units", null, String.class);
    public static final ConfigOption<String> LIGHT_HIDDEN_UNITS = new ConfigOption<>(
            "light_hidden_units", "64,32", "RocketLaunching light hidden units", null, String.class);
    public static final ConfigOption<String> SHARE_HIDDEN_UNITS = new ConfigOption<>(
            "share_hidden_units", null, "Optional RocketLaunching shared hidden units", null, String.class);

    public static final ConfigOption<String> HIDDEN_UNITS = new ConfigOption<>(
            "hidden_units",
            "512,256,128", "Hidden units for deep network", null, String.class);

    public static final ConfigOption<Integer> NUM_EXPERT = new ConfigOption<>(
            "num_expert", 3, "Number of MMoE experts", null, Integer.class);
    public static final ConfigOption<String> EXPERT_HIDDEN_UNITS = new ConfigOption<>(
            "expert_hidden_units", "256,128", "MMoE expert hidden units", null, String.class);
    public static final ConfigOption<String> TASK_HIDDEN_UNITS = new ConfigOption<>(
            "task_hidden_units", "64,32", "Default task tower hidden units", null, String.class);
    public static final ConfigOption<String> EVAL_INPUT_PATH = new ConfigOption<>(
            "eval_input_path", null, "Parquet evaluation data path", null, String.class);

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
