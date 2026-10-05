package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Restricts RocketLaunching to the supported native distillation configuration. */
final class RocketLaunchingOptions {
    private static final Set<String> UNUSED = Set.of("hidden_units", "user_features", "item_features",
            "user_hidden_units", "item_hidden_units", "output_dim", "num_expert", "expert_hidden_units",
            "task_hidden_units", "loss", "num_class", "teacher_checkpoint", "export_type");

    private RocketLaunchingOptions() {}

    static String hiddenUnits(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Hidden units must be a nonempty positive integer list");
        return Arrays.stream(value.split(",", -1)).map(unit -> {
            int number = Integer.parseInt(unit.trim());
            if (number <= 0) throw new IllegalArgumentException("Hidden units must be positive");
            return Integer.toString(number);
        }).collect(Collectors.joining(","));
    }

    static void validate(ModelConf model) {
        Map<String, String> params = model.getParams() == null ? Map.of() : model.getParams();
        for (String key : params.keySet()) {
            if (UNUSED.contains(key) || key.startsWith("task.")) {
                throw new IllegalArgumentException("RocketLaunching does not support option " + key);
            }
        }
        if (params.containsKey("feature_based_distillation") && !"true".equals(params.get("feature_based_distillation"))) {
            throw new IllegalArgumentException("RocketLaunching requires feature_based_distillation=true");
        }
        if (params.containsKey("feature_distillation_function") && !"COSINE".equals(params.get("feature_distillation_function"))) {
            throw new IllegalArgumentException("RocketLaunching requires COSINE feature distillation");
        }
        String booster = hiddenUnits(Config.BOOSTER_HIDDEN_UNITS.getValue(params));
        String light = hiddenUnits(Config.LIGHT_HIDDEN_UNITS.getValue(params));
        Set<String> widths = Set.copyOf(Arrays.asList(booster.split(",")));
        if (Arrays.stream(light.split(",")).noneMatch(widths::contains)) {
            throw new IllegalArgumentException("RocketLaunching booster and light need at least one matching hidden width");
        }
        if (params.containsKey("share_hidden_units")) hiddenUnits(params.get("share_hidden_units"));
        String label = params.get("label_columns").trim();
        FieldSchema field = model.getInputFields().stream().filter(input -> input.getName().equals(label)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("RocketLaunching label must be declared as a numeric scalar"));
        if (!FieldTypeUtils.isInteger(field.getType()) && !FieldTypeUtils.isFloat(field.getType())) {
            throw new IllegalArgumentException("RocketLaunching label must be a numeric scalar with 0/1 values");
        }
        if (model.getInputFields().stream().anyMatch(input -> input.getName().equalsIgnoreCase("probs_light")
                || input.getName().equalsIgnoreCase("logits_light"))) {
            throw new IllegalArgumentException("RocketLaunching input names cannot collide with light outputs");
        }
    }
}
