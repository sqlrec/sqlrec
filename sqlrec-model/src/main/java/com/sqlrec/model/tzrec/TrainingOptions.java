package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Validates training observation settings and emits native training metrics. */
final class TrainingOptions {
    private static final Set<String> SUMMARIES = Set.of(
            "loss", "learning_rate", "global_gradient_norm", "gradient_norm", "parameter", "gradient");

    private TrainingOptions() {
    }

    static void validate(ModelConf model, Map<String, String> params) {
        validateObservationSettings(params);
        for (var option : List.of(Config.BATCH_SIZE, Config.NUM_EPOCHS, Config.NNODES, Config.NPROC_PER_NODE)) {
            if (option.getValue(params) <= 0) {
                throw new IllegalArgumentException(option.getKey() + " must be positive");
            }
        }
        if (Config.NUM_WORKERS.getValue(params) < 0) {
            throw new IllegalArgumentException("num_workers must be nonnegative");
        }
        int port = Config.MASTER_PORT.getValue(params);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("master_port must be in [1,65535]");
        }
        for (var option : List.of(Config.SPARSE_LR, Config.DENSE_LR)) {
            double value = option.getValue(params);
            if (!Double.isFinite(value) || value <= 0) {
                throw new IllegalArgumentException(option.getKey() + " must be finite and positive");
            }
        }
        String precision = Config.MIXED_PRECISION.getValueOrNull(params);
        if (precision != null && !precision.isEmpty()
                && !Set.of("BF16", "FP16").contains(precision.trim().toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("mixed_precision must be BF16 or FP16");
        }
        if ("tzrec.mmoe".equals(model.getParams().get("model"))) {
            MultiTaskOptions.parse(model, params);
        }
        if (!"tzrec.dssm".equals(model.getParams().get("model"))) {
            return;
        }
        String mode = Config.NEGATIVE_SAMPLING.getValue(params);
        if (!Set.of("in_batch", "pool").contains(mode)) {
            throw new IllegalArgumentException("negative_sampling must be in_batch or pool");
        }
        if (mode.equals("pool")) {
            validatePoolSampling(model, params);
        }
    }

    private static void validatePoolSampling(ModelConf model, Map<String, String> params) {
        if (Config.NEGATIVE_SAMPLE_COUNT.getValue(params) <= 0) {
            throw new IllegalArgumentException("negative_sample_count must be positive");
        }
        if (Config.NEGATIVE_POOL_SIZE.getValue(params) <= Config.BATCH_SIZE.getValue(params)) {
            throw new IllegalArgumentException("negative_pool_size must exceed batch_size");
        }
        String itemId = Config.ITEM_ID_COLUMN.getValue(params);
        List<String> users = FieldTypeUtils.parseCsvList(Config.USER_FEATURES.getValueOrNull(model.getParams()));
        List<String> items = FieldTypeUtils.parseCsvList(Config.ITEM_FEATURES.getValueOrNull(model.getParams()));
        List<FieldSchema> features = FeatureOptions.features(model);
        if (items.isEmpty()) {
            items = features.stream()
                    .map(FieldSchema::getName)
                    .filter(name -> !users.contains(name))
                    .toList();
        }
        if (!Collections.disjoint(users, items)) {
            throw new IllegalArgumentException("Pool sampling requires disjoint user_features and item_features");
        }
        FieldSchema id = features.stream()
                .filter(field -> field.getName().equals(itemId))
                .findFirst()
                .orElse(null);
        if (!items.contains(itemId) || id == null
                || !(FieldTypeUtils.isInteger(id.getType()) || FieldTypeUtils.isString(id.getType()))) {
            throw new IllegalArgumentException("item_id_column must be a scalar STRING, INT or BIGINT item feature");
        }
    }

    static List<String> summaries(Map<String, String> params) {
        List<String> summaries = Arrays.stream(Config.TENSORBOARD_SUMMARIES.getValue(params).split(",", -1))
                .map(String::trim).toList();
        if (!SUMMARIES.containsAll(summaries) || new HashSet<>(summaries).size() != summaries.size()) {
            throw new IllegalArgumentException("tensorboard_summaries must contain unique native summary types: "
                    + "loss,learning_rate,global_gradient_norm,gradient_norm,parameter,gradient");
        }
        return summaries;
    }

    static int decayStep(Map<String, String> params) {
        return Config.TRAIN_METRIC_DECAY_STEP.getValue(params);
    }

    static void validateModelDir(Map<String, String> params, String modelDir) {
        if (Config.IS_PROFILING.getValue(params) && modelDir != null
                && modelDir.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*") && !modelDir.startsWith("file:")) {
            throw new IllegalArgumentException("Native profiling requires a local or mounted model_dir; remote URIs are unsupported");
        }
    }

    private static void validateObservationSettings(Map<String, String> params) {
        Config.USE_TENSORBOARD.getValue(params);
        Config.IS_PROFILING.getValue(params);
        summaries(params);
        boolean trainingMetrics = Config.ENABLE_TRAIN_METRICS.getValue(params);
        int logSteps = Config.LOG_STEP_COUNT_STEPS.getValue(params);
        if (logSteps <= 0) {
            throw new IllegalArgumentException("log_step_count_steps must be positive");
        }
        int checkpointSteps = Config.SAVE_CHECKPOINTS_STEPS.getValue(params);
        if (checkpointSteps < 0) {
            throw new IllegalArgumentException("save_checkpoints_steps must be nonnegative");
        }
        Integer epochs = Config.SAVE_CHECKPOINTS_EPOCHS.getValueOrNull(params);
        if (epochs != null && epochs < 0) {
            throw new IllegalArgumentException("save_checkpoints_epochs must be nonnegative");
        }
        if (Config.SAVE_CHECKPOINTS_STEPS.isSet(params) && checkpointSteps > 0 && epochs != null && epochs > 0) {
            throw new IllegalArgumentException("save_checkpoints_steps and save_checkpoints_epochs cannot both be positive");
        }
        int decaySteps = decayStep(params);
        if (decaySteps <= 0 || trainingMetrics && decaySteps % logSteps != 0) {
            throw new IllegalArgumentException("train_metric_decay_step must be a positive multiple of log_step_count_steps");
        }
        double rate = Config.TRAIN_METRIC_DECAY_RATE.getValue(params);
        if (!Double.isFinite(rate) || rate < 0 || rate >= 1) {
            throw new IllegalArgumentException("train_metric_decay_rate must be finite and in [0,1)");
        }
    }

    static void addMetric(StringBuilder config, Map<String, String> params, String metric, String indent) {
        if (!Config.ENABLE_TRAIN_METRICS.getValue(params)) {
            return;
        }
        config.append(indent).append("train_metrics { ").append(metric);
        if (Config.TRAIN_METRIC_DECAY_STEP.isSet(params)) {
            config.append(" decay_step: ").append(decayStep(params));
        }
        if (Config.TRAIN_METRIC_DECAY_RATE.isSet(params)) {
            config.append(" decay_rate: ").append(Config.TRAIN_METRIC_DECAY_RATE.getValue(params));
        }
        config.append(" }\n");
    }
}
