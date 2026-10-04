package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Resolves feature roles and validates the feature contract shared by training and serving. */
final class FeatureOptions {
    private static final Pattern DECIMAL = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private FeatureOptions() {}

    static boolean isRaw(String type) {
        type = compactType(type);
        return FieldTypeUtils.isFloat(type) || "array<float>".equalsIgnoreCase(type)
                || "array<double>".equalsIgnoreCase(type);
    }

    private static String compactType(String type) {
        return type == null ? null : type.replaceAll("\\s+", "");
    }

    static List<FieldSchema> features(ModelConf model) {
        List<String> labels = FieldTypeUtils.parseCsvList(Config.LABEL_COLUMNS.getValueOrNull(model.getParams()));
        List<FieldSchema> result = new ArrayList<>();
        if (model.getInputFields() != null) {
            for (FieldSchema field : model.getInputFields()) {
                if (!labels.contains(field.getName())) result.add(field);
            }
        }
        return result;
    }

    static List<String> sparseFeatures(ModelConf model) {
        List<String> result = new ArrayList<>();
        for (FieldSchema field : features(model)) {
            if (!isRaw(field.getType()) || !boundaries(field.getName(), model.getParams()).isEmpty()) {
                result.add(field.getName());
            }
        }
        return result;
    }

    static int embeddingDim(String name, Map<String, String> params) {
        return Integer.parseInt(params.getOrDefault("column." + name + ".embedding_dim",
                String.valueOf(Config.EMBEDDING_DIM.getValue(params))));
    }

    static int valueDim(FieldSchema field, Map<String, String> params) {
        return Integer.parseInt(params.getOrDefault("column." + field.getName() + ".value_dim", "1"));
    }

    static String defaultValue(FieldSchema field, Map<String, String> params) {
        String separator = params.getOrDefault("column." + field.getName() + ".separator", "\035");
        return params.getOrDefault("column." + field.getName() + ".default_value",
                String.join(separator, java.util.Collections.nCopies(valueDim(field, params), "0")));
    }

    static List<Float> boundaries(String name, Map<String, String> params) {
        List<Float> result = new ArrayList<>();
        String text = params.get("column." + name + ".boundaries");
        if (text != null) {
            for (String token : text.split(",", -1)) {
                float value = finiteFloat(token);
                if (!Float.isFinite(value) || (!result.isEmpty() && value <= result.get(result.size() - 1))) {
                    throw new IllegalArgumentException("column." + name + ".boundaries must be finite and strictly increasing");
                }
                result.add(value);
            }
        }
        return result;
    }

    static void validateNormalizer(String text) {
        if (text == null || text.isEmpty()) return;
        Map<String, String> parts = new java.util.LinkedHashMap<>();
        for (String part : text.split(",", -1)) {
            String[] pair = part.trim().split("=", -1);
            if (pair.length != 2 || parts.put(pair[0], pair[1]) != null) {
                throw new IllegalArgumentException("Invalid normalizer: " + text);
            }
        }
        String method = parts.remove("method");
        Set<String> keys;
        if ("zscore".equals(method)) keys = Set.of("mean", "standard_deviation");
        else if ("minmax".equals(method)) keys = Set.of("min", "max");
        else if ("log10".equals(method)) keys = Set.of("threshold", "default");
        else throw new IllegalArgumentException("Normalizer must use zscore, minmax or log10");
        if (!keys.containsAll(parts.keySet()) || (!"log10".equals(method) && !parts.keySet().equals(keys))) {
            throw new IllegalArgumentException("Invalid normalizer parameters: " + text);
        }
        for (String number : parts.values()) {
            finiteFloat(number);
        }
        if ("zscore".equals(method) && finiteFloat(parts.get("standard_deviation")) <= 0
                || "minmax".equals(method) && finiteFloat(parts.get("max")) <= finiteFloat(parts.get("min"))
                || "log10".equals(method) && finiteFloat(parts.getOrDefault("threshold", "1e-10")) <= 0) {
            throw new IllegalArgumentException("Normalizer scale must be positive");
        }
        if ("minmax".equals(method) && !Float.isFinite(finiteFloat(parts.get("max")) - finiteFloat(parts.get("min")))) {
            throw new IllegalArgumentException("Normalizer scale must be finite float32");
        }
    }

    static String validate(ModelConf model, boolean dssm, boolean deepfm) {
        return validate(model, dssm, deepfm, false);
    }

    static String validate(ModelConf model, boolean dssm, boolean deepfm, boolean multiTask) {
        try {
            Map<String, String> params = model.getParams() == null ? Map.of() : model.getParams();
            List<String> labels = FieldTypeUtils.parseCsvList(params.get("label_columns"));
            if (multiTask) MultiTaskOptions.parse(model);
            else if (labels.size() != 1) return "TZRec requires exactly one label_columns entry";
            Set<String> names = new HashSet<>();
            if (model.getInputFields() == null) return "TZRec requires input fields";
            for (FieldSchema field : model.getInputFields()) {
                if (field.getName() == null || field.getName().isEmpty() || field.getName().contains(":") || !names.add(field.getName())) {
                    return "Input field names must be nonempty, unique and contain no ':'";
                }
            }
            List<FieldSchema> features = features(model);
            if (features.isEmpty()) return "TZRec requires at least one non-label feature";
            for (String key : params.keySet()) {
                if (!key.startsWith("column.")) continue;
                boolean recognized = false;
                for (FieldSchema field : features) {
                    String prefix = "column." + field.getName() + ".";
                    if (!key.startsWith(prefix)) continue;
                    Set<String> options = isRaw(field.getType())
                            ? Set.of("embedding_dim", "value_dim", "separator", "default_value", "boundaries", "normalizer", "embedding", "autodis.num_channels")
                            : Set.of("bucket_size", "embedding_dim", "default_value", "separator");
                    recognized = options.contains(key.substring(prefix.length()));
                    if (recognized) break;
                }
                if (!recognized) return "Unknown or inapplicable TZRec feature option: " + key;
            }
            positive(Config.NUM_BUCKETS.getValue(params), "num_buckets");
            positive(Config.EMBEDDING_DIM.getValue(params), "embedding_dim");
            Set<Integer> fmDims = new HashSet<>();
            for (FieldSchema field : features) {
                String type = compactType(field.getType());
                String prefix = "column." + field.getName() + ".";
                if (isRaw(type)) {
                    int dim = valueDim(field, params);
                    positive(dim, prefix + "value_dim");
                    if (dim > 65536) return prefix + "value_dim must not exceed 65536";
                    if (FieldTypeUtils.isArray(type) && !params.containsKey(prefix + "value_dim")) {
                        return prefix + "value_dim is required for float vectors";
                    }
                    if (!FieldTypeUtils.isArray(type) && dim != 1) return prefix + "value_dim must be 1 for scalar floats";
                    String separator = params.getOrDefault(prefix + "separator", "\035");
                    if (separator.isEmpty()) return prefix + "separator must be nonempty";
                    String defaults = defaultValue(field, params);
                    List<Float> bounds = boundaries(field.getName(), params);
                    if (defaults.isEmpty() && bounds.isEmpty()) return prefix + "default_value is required for dense features";
                    if (!defaults.isEmpty()) {
                        String[] tokens = defaults.split(Pattern.quote(separator), -1);
                        if (tokens.length != dim) return prefix + "default_value must match value_dim";
                        for (String token : tokens) {
                            finiteFloat(token);
                        }
                    }
                    validateNormalizer(params.get(prefix + "normalizer"));
                    String embedding = params.getOrDefault(prefix + "embedding", "none");
                    if (!Set.of("none", "mlp", "autodis").contains(embedding)) return prefix + "embedding must be none, mlp or autodis";
                    if (embedding.equals("autodis") && dim != 1) return prefix + "embedding=autodis requires value_dim=1";
                    if (!bounds.isEmpty() && !embedding.equals("none")) return "boundaries and dense embedding cannot be combined";
                    if (!bounds.isEmpty() || !embedding.equals("none")) positive(embeddingDim(field.getName(), params), prefix + "embedding_dim");
                    if (bounds.isEmpty() && embedding.equals("none") && params.containsKey(prefix + "embedding_dim")) return prefix + "embedding_dim requires boundaries or embedding=mlp/autodis";
                    if (embedding.equals("autodis")) positive(Integer.parseInt(params.getOrDefault(prefix + "autodis.num_channels", "3")), prefix + "autodis.num_channels");
                    else if (params.containsKey(prefix + "autodis.num_channels")) return prefix + "autodis.num_channels requires embedding=autodis";
                    if (!bounds.isEmpty()) fmDims.add(embeddingDim(field.getName(), params));
                    if (!bounds.isEmpty() && embeddingDim(field.getName(), params) % 4 != 0) return "Sparse embedding_dim must be a multiple of 4";
                } else {
                    if (!FieldTypeUtils.isInteger(type) && !FieldTypeUtils.isString(type) && !"array<string>".equalsIgnoreCase(type)) {
                        return "Unsupported TZRec feature type for " + field.getName() + ": " + type + "; use ARRAY<STRING> for multivalue IDs";
                    }
                    int buckets = Integer.parseInt(params.getOrDefault(prefix + "bucket_size", String.valueOf(Config.NUM_BUCKETS.getValue(params))));
                    positive(buckets, prefix + "bucket_size");
                    positive(embeddingDim(field.getName(), params), prefix + "embedding_dim");
                    if (embeddingDim(field.getName(), params) % 4 != 0) return "Sparse embedding_dim must be a multiple of 4";
                    String separator = params.getOrDefault(prefix + "separator", "\035");
                    if (separator.isEmpty()) return prefix + "separator must be nonempty";
                    String defaults = params.getOrDefault(prefix + "default_value", "");
                    if (!defaults.isEmpty()) {
                        for (String token : defaults.split(Pattern.quote(separator), -1)) {
                            if (token.isEmpty()) return prefix + "default_value must not contain empty tokens";
                            if (FieldTypeUtils.isInteger(type)) {
                                long id = Long.parseLong(token.trim());
                                if (id < 0 || id >= buckets) return prefix + "default_value is outside the bucket range";
                            }
                        }
                    }
                    if (!FieldTypeUtils.isInteger(type) && !Config.USE_FARM_HASH_TO_BUCKETIZE.getValue(params).equalsIgnoreCase("true")) return "Hash features require USE_FARM_HASH_TO_BUCKETIZE=true";
                    fmDims.add(embeddingDim(field.getName(), params));
                }
            }
            if (!dssm && !multiTask && sparseFeatures(model).isEmpty()) return "Ranking models require at least one sparse feature for the wide group";
            if (deepfm && fmDims.size() != 1) return "DeepFM sparse feature embedding dimensions must match";
            if (dssm) {
                Set<String> available = new HashSet<>();
                for (FieldSchema field : features) available.add(field.getName());
                List<String> user = FieldTypeUtils.parseCsvList(params.get("user_features"));
                List<String> item = FieldTypeUtils.parseCsvList(params.get("item_features"));
                if (user.isEmpty() && item.isEmpty()) return "At least one of user_features or item_features is required for DSSM model";
                for (List<String> tower : List.of(user, item)) {
                    if (new HashSet<>(tower).size() != tower.size() || !available.containsAll(tower)) return "DSSM tower features must be unique, declared and must not include labels";
                }
                if (!user.isEmpty() && !item.isEmpty()) {
                    Set<String> assigned = new HashSet<>(user); assigned.addAll(item);
                    if (!assigned.equals(available)) return "DSSM towers must cover all non-label input features";
                }
                if (user.isEmpty() && available.size() == item.size() || item.isEmpty() && available.size() == user.size()) return "Both DSSM towers require at least one feature";
                positive(Config.OUTPUT_DIM.getValue(params), "output_dim");
                hiddenUnits(Config.USER_HIDDEN_UNITS.getValue(params));
                hiddenUnits(Config.ITEM_HIDDEN_UNITS.getValue(params));
            } else if (!multiTask) hiddenUnits(Config.HIDDEN_UNITS.getValue(params));
            positive(Config.BATCH_SIZE.getValue(params), "batch_size");
            if (Config.NUM_WORKERS.getValue(params) < 0) return "num_workers must be nonnegative";
            positive(Config.NUM_EPOCHS.getValue(params), "num_epochs");
            positive(Config.NNODES.getValue(params), "nnodes");
            positive(Config.NPROC_PER_NODE.getValue(params), "nproc_per_node");
            positive(Config.POD_CPU_CORES.getValue(params), "pod_cpu_cores");
            int port = Config.MASTER_PORT.getValue(params);
            if (port < 1 || port > 65535) return "master_port must be in [1,65535]";
            for (double lr : List.of(Config.SPARSE_LR.getValue(params), Config.DENSE_LR.getValue(params))) {
                if (!Double.isFinite(lr) || lr <= 0) return "Learning rates must be finite and positive";
            }
            String precision = Config.MIXED_PRECISION.getValueOrNull(params);
            if (precision != null && !precision.isEmpty() && !Set.of("BF16", "FP16").contains(precision.trim().toUpperCase(java.util.Locale.ROOT))) return "mixed_precision must be BF16 or FP16";
            return null;
        } catch (IllegalArgumentException e) {
            return "Invalid TZRec configuration: " + e.getMessage();
        }
    }

    private static void hiddenUnits(String value) {
        for (String unit : value.split(",", -1)) positive(Integer.parseInt(unit.trim()), "hidden_units");
    }

    private static float finiteFloat(String value) {
        if (!DECIMAL.matcher(value.trim()).matches()) throw new IllegalArgumentException("Expected a decimal float32 number: " + value);
        double number = Double.parseDouble(value.trim());
        if (!Double.isFinite(number) || Math.abs(number) > (double) Float.MAX_VALUE) {
            throw new IllegalArgumentException("Number must be finite float32: " + value);
        }
        return (float) number;
    }

    private static void positive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    }
}
