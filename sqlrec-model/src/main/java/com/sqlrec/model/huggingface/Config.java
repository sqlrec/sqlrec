package com.sqlrec.model.huggingface;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.model.common.ModelConfigBase;

import java.util.List;

/** Configuration options for the Hugging Face Transformers backend. */
public class Config extends ModelConfigBase {
    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
            "image", "sqlrec/transformers", "Transformers runtime image", null, String.class);
    // Deliberately has no -cpu suffix: the same image is used on CPU and GPU nodes.
    public static final ConfigOption<String> VERSION = new ConfigOption<>(
            "version", SqlRecConfigs.SQLREC_VERSION.getValue(), "Runtime image version", null, String.class);

    public static final ConfigOption<String> TASK = new ConfigOption<>(
            "task", null, "Transformers task", List.of(
                    "text-classification", "text-generation", "embedding", "image-embedding"), String.class);
    public static final ConfigOption<String> REPO_ID = new ConfigOption<>(
            "repo_id", null, "Hugging Face Hub model repository", null, String.class);
    public static final ConfigOption<String> REVISION = new ConfigOption<>(
            "revision", "main", "Hub branch, tag, or commit", null, String.class);
    public static final ConfigOption<String> TEXT_COLUMN = new ConfigOption<>(
            "text_column", null, "Text input column", null, String.class);
    public static final ConfigOption<String> TEXT_PAIR_COLUMN = new ConfigOption<>(
            "text_pair_column", null, "Optional paired text column", null, String.class);
    public static final ConfigOption<String> PROMPT_COLUMN = new ConfigOption<>(
            "prompt_column", null, "Text generation prompt column", null, String.class);
    public static final ConfigOption<String> IMAGE_COLUMN = new ConfigOption<>(
            "image_column", null, "Image URL column", null, String.class);
    public static final ConfigOption<String> POOLING = new ConfigOption<>(
            "pooling", "mean", "Embedding pooling strategy", List.of("mean", "cls", "last_token", "pooler"), String.class);
    public static final ConfigOption<Boolean> NORMALIZE = new ConfigOption<>(
            "normalize", true, "L2 normalize embeddings", null, Boolean.class);
    public static final ConfigOption<Integer> MAX_LENGTH = new ConfigOption<>(
            "max_length", 512, "Tokenizer maximum sequence length", null, Integer.class);
    public static final ConfigOption<Boolean> TRUST_REMOTE_CODE = new ConfigOption<>(
            "trust_remote_code", false, "Allow code shipped by the model repository", null, Boolean.class);
    public static final ConfigOption<String> ALLOW_PATTERNS = new ConfigOption<>(
            "allow_patterns", null, "Comma-separated snapshot allow patterns", null, String.class);
    public static final ConfigOption<String> IGNORE_PATTERNS = new ConfigOption<>(
            "ignore_patterns", null, "Comma-separated snapshot ignore patterns", null, String.class);
    public static final ConfigOption<Boolean> FORCE_DOWNLOAD = new ConfigOption<>(
            "force_download", false, "Force downloading the Hub snapshot", null, Boolean.class);
    public static final ConfigOption<String> HF_TOKEN_SECRET = new ConfigOption<>(
            "hf_token_secret", null, "Kubernetes Secret containing HF_TOKEN", null, String.class);
    public static final ConfigOption<String> HF_TOKEN_SECRET_KEY = new ConfigOption<>(
            "hf_token_secret_key", "token", "Key in the Hugging Face token Secret", null, String.class);

    public static final ConfigOption<String> DEVICE = new ConfigOption<>(
            "device", "auto", "Inference device", List.of("auto", "cpu", "cuda"), String.class);
    public static final ConfigOption<String> DTYPE = new ConfigOption<>(
            "dtype", "auto", "Inference dtype", List.of("auto", "float32", "float16", "bfloat16"), String.class);
    public static final ConfigOption<Integer> INFERENCE_BATCH_SIZE = new ConfigOption<>(
            "inference_batch_size", 8, "Internal inference batch size", null, Integer.class);
    public static final ConfigOption<Integer> MAX_NEW_TOKENS = new ConfigOption<>(
            "max_new_tokens", 128, "Maximum generated tokens", null, Integer.class);
    public static final ConfigOption<Boolean> DO_SAMPLE = new ConfigOption<>(
            "do_sample", false, "Enable sampling for text generation", null, Boolean.class);
    public static final ConfigOption<Double> TEMPERATURE = new ConfigOption<>(
            "temperature", 1.0, "Generation temperature", null, Double.class);
    public static final ConfigOption<Double> TOP_P = new ConfigOption<>(
            "top_p", 1.0, "Generation top-p", null, Double.class);
    public static final ConfigOption<Integer> POD_GPU = new ConfigOption<>(
            "pod_gpu", 0, "Number of GPUs requested by the serving pod", null, Integer.class);
    public static final ConfigOption<String> POD_GPU_RESOURCE = new ConfigOption<>(
            "pod_gpu_resource", "nvidia.com/gpu", "Kubernetes GPU resource name", null, String.class);

    public static final ConfigOption<String> IMAGE_URL_ALLOWED_HOSTS = new ConfigOption<>(
            "image_url_allowed_hosts", "", "Comma-separated image URL host allowlist", null, String.class);
    public static final ConfigOption<Integer> IMAGE_DOWNLOAD_TIMEOUT_MS = new ConfigOption<>(
            "image_download_timeout_ms", 5000, "Image URL timeout", null, Integer.class);
    public static final ConfigOption<Integer> IMAGE_MAX_BYTES = new ConfigOption<>(
            "image_max_bytes", 10 * 1024 * 1024, "Maximum downloaded image size", null, Integer.class);
    public static final ConfigOption<Integer> IMAGE_MAX_PIXELS = new ConfigOption<>(
            "image_max_pixels", 20_000_000, "Maximum decoded image pixels", null, Integer.class);

    private Config() {
    }
}
