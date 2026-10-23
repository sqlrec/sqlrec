package com.sqlrec.model.tzrec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates training observation settings and emits native training metrics. */
final class TrainingOptions {
    private static final Set<String> SUMMARIES = Set.of(
            "loss", "learning_rate", "global_gradient_norm", "gradient_norm", "parameter", "gradient");

    private TrainingOptions() {}

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

    static void validate(Map<String, String> params) {
        Config.USE_TENSORBOARD.getValue(params);
        Config.IS_PROFILING.getValue(params);
        summaries(params);
        boolean trainingMetrics = Config.ENABLE_TRAIN_METRICS.getValue(params);
        int logSteps = Config.LOG_STEP_COUNT_STEPS.getValue(params);
        if (logSteps <= 0) throw new IllegalArgumentException("log_step_count_steps must be positive");
        int checkpointSteps = Config.SAVE_CHECKPOINTS_STEPS.getValue(params);
        if (checkpointSteps < 0) {
            throw new IllegalArgumentException("save_checkpoints_steps must be nonnegative");
        }
        Integer epochs = Config.SAVE_CHECKPOINTS_EPOCHS.getValueOrNull(params);
        if (epochs != null && epochs < 0) throw new IllegalArgumentException("save_checkpoints_epochs must be nonnegative");
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
        if (!Config.ENABLE_TRAIN_METRICS.getValue(params)) return;
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
