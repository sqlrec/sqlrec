package com.sqlrec.model.tzrec;

import java.util.List;
import java.util.Map;

/** Native metrics that match SQLRec's scalar task outputs. */
final class MetricOptions {
    static final List<String> BINARY = List.of("auc", "normalized_entropy", "accuracy");
    static final List<String> REGRESSION = List.of("mean_squared_error", "mean_absolute_error");
    static final List<Integer> RECALL_TOP_K = List.of(1, 5, 10, 20);

    private MetricOptions() {}

    static void addMetrics(StringBuilder config, Map<String, String> params, List<String> metrics, String indent) {
        for (String metric : metrics) {
            config.append(indent).append("metrics { ").append(metric).append(" {} }\n");
        }
        for (String metric : metrics) {
            // Native TrainMetricConfig does not support normalized entropy.
            if (!metric.equals("normalized_entropy")) {
                TrainingOptions.addMetric(config, params, metric + " {}", indent);
            }
        }
    }
}
