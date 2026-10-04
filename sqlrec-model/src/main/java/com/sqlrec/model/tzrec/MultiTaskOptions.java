package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** One ordered task contract for validation, pipeline generation and SQL outputs. */
final class MultiTaskOptions {
    private MultiTaskOptions() {}

    record TaskSpec(String label, String type, String hiddenUnits, float weight, List<String> metrics) {
        String outputName() {
            return (type.equals("binary") ? "probs_" : "y_") + label;
        }
    }

    static List<TaskSpec> parse(ModelConf model) {
        Map<String, String> params = model.getParams() == null ? Map.of() : model.getParams();
        List<String> labels = csv(Config.LABEL_COLUMNS.getValue(params), "label_columns");
        if (labels.size() < 2) throw new IllegalArgumentException("MMoE requires at least two labels");
        Set<String> folded = new HashSet<>();
        for (String label : labels) {
            if (!label.matches("[A-Za-z_][A-Za-z0-9_]*") || !folded.add(label.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Task labels must be unique identifiers: " + label);
            }
        }
        if (model.getInputFields() == null) throw new IllegalArgumentException("MMoE requires input fields");
        folded.clear();
        for (FieldSchema field : model.getInputFields()) {
            if (field.getName() == null || !folded.add(field.getName().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Input fields must be unique ignoring case");
            }
        }
        positive(Config.NUM_EXPERT.getValue(params), "num_expert");
        hiddenUnits(Config.EXPERT_HIDDEN_UNITS.getValue(params));
        String defaultUnits = hiddenUnits(Config.TASK_HIDDEN_UNITS.getValue(params));
        for (String key : params.keySet()) {
            if (Set.of("hidden_units", "user_features", "item_features", "user_hidden_units", "item_hidden_units", "output_dim").contains(key)) {
                throw new IllegalArgumentException("Inapplicable MMoE option: " + key);
            }
            if (!key.startsWith("task.")) continue;
            String[] parts = key.split("\\.", -1);
            if (parts.length != 3 || !labels.contains(parts[1])
                    || !Set.of("type", "hidden_units", "weight", "metrics").contains(parts[2])) {
                throw new IllegalArgumentException("Unknown MMoE task option: " + key);
            }
        }
        List<TaskSpec> tasks = new ArrayList<>();
        for (String label : labels) {
            FieldSchema field = model.getInputFields().stream().filter(f -> label.equals(f.getName())).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Label is not declared: " + label));
            if (!FieldTypeUtils.isInteger(field.getType()) && !FieldTypeUtils.isFloat(field.getType())) {
                throw new IllegalArgumentException("Task label must be a numeric scalar: " + label);
            }
            String prefix = "task." + label + ".";
            String type = params.getOrDefault(prefix + "type", "binary");
            if (!Set.of("binary", "regression").contains(type)) throw new IllegalArgumentException(prefix + "type must be binary or regression");
            if (type.equals("regression") && !FieldTypeUtils.isFloat(field.getType())) {
                throw new IllegalArgumentException("Regression label must be FLOAT or DOUBLE: " + label);
            }
            float weight = Float.parseFloat(params.getOrDefault(prefix + "weight", "1.0"));
            if (!Float.isFinite(weight) || weight <= 0) throw new IllegalArgumentException(prefix + "weight must be finite and positive float32");
            List<String> metrics = csv(params.getOrDefault(prefix + "metrics", type.equals("binary") ? "auc" : "mean_squared_error"), prefix + "metrics");
            Set<String> allowed = type.equals("binary") ? Set.of("auc", "accuracy") : Set.of("mean_squared_error", "mean_absolute_error");
            if (!allowed.containsAll(metrics) || new HashSet<>(metrics).size() != metrics.size()) throw new IllegalArgumentException("Metrics do not match task type or are duplicated: " + label);
            TaskSpec task = new TaskSpec(label, type, hiddenUnits(params.getOrDefault(prefix + "hidden_units", defaultUnits)), weight, List.copyOf(metrics));
            if (folded.contains(task.outputName().toLowerCase(Locale.ROOT))) throw new IllegalArgumentException("Task output conflicts with input: " + task.outputName());
            tasks.add(task);
        }
        return List.copyOf(tasks);
    }

    static String hiddenUnits(String value) {
        return csv(value, "hidden_units").stream().map(unit -> {
            int size = Integer.parseInt(unit);
            positive(size, "hidden_units");
            return Integer.toString(size);
        }).collect(Collectors.joining(","));
    }

    private static List<String> csv(String value, String name) {
        List<String> result = Arrays.stream(value.split(",", -1)).map(String::trim).toList();
        if (result.contains("")) throw new IllegalArgumentException(name + " must contain nonempty entries");
        return result;
    }

    private static void positive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    }
}
