package com.sqlrec.udf.llm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.Strictness;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Separates API body parameters from local execution options. */
final class LlmOptions {
    private static final Gson JSON = new GsonBuilder().setStrictness(Strictness.STRICT).create();
    final JsonObject request;
    final int timeoutMs;

    private LlmOptions(JsonObject request, int timeoutMs) {
        this.request = request;
        this.timeoutMs = timeoutMs;
    }

    static LlmOptions parse(String json, String... allowedRequestKeys) {
        JsonObject root;
        try {
            root = JSON.fromJson(json == null ? "{}" : json, JsonObject.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("options_json must be a JSON object");
        }
        if (root == null) {
            throw new IllegalArgumentException("options_json must be a JSON object");
        }
        checkKeys(root, "options", "request", "runtime");
        JsonObject request = object(root, "request");
        JsonObject runtime = object(root, "runtime");
        checkKeys(request, "request", allowedRequestKeys);
        checkKeys(runtime, "runtime", "timeout_ms");
        int timeout = runtime.has("timeout_ms") ? positiveInt(runtime, "timeout_ms") : 30000;
        if (timeout > 300000) {
            throw new IllegalArgumentException("timeout_ms must not exceed 300000");
        }
        if (request.has("dimensions")) {
            positiveInt(request, "dimensions");
        }
        if (request.has("max_completion_tokens")) {
            positiveInt(request, "max_completion_tokens");
        }
        range(request, "temperature", 0, 2);
        range(request, "top_p", 0, 1);
        for (String key : Arrays.asList("instructions", "user", "reasoning_effort")) {
            if (request.has(key)) {
                string(request, key);
            }
        }
        return new LlmOptions(request, timeout);
    }

    private static JsonObject object(JsonObject parent, String key) {
        if (!parent.has(key)) {
            return new JsonObject();
        }
        if (!parent.get(key).isJsonObject()) {
            throw new IllegalArgumentException(key + " must be a JSON object");
        }
        return parent.getAsJsonObject(key);
    }

    private static void checkKeys(JsonObject object, String section, String... keys) {
        Set<String> allowed = new HashSet<>(Arrays.asList(keys));
        if (!allowed.containsAll(object.keySet())) {
            throw new IllegalArgumentException("Unsupported parameter in " + section);
        }
    }

    static int positiveInt(JsonObject object, String key) {
        try {
            JsonElement value = object.get(key);
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
                throw new ArithmeticException();
            }
            int result = new BigDecimal(value.getAsString()).intValueExact();
            if (result <= 0) {
                throw new ArithmeticException();
            }
            return result;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(key + " must be a positive integer");
        }
    }

    private static String string(JsonObject object, String key) {
        JsonElement value = object.get(key);
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        return value.getAsString();
    }

    private static void range(JsonObject object, String key, double min, double max) {
        if (!object.has(key)) {
            return;
        }
        JsonElement value = object.get(key);
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException(key + " must be a number");
        }
        double number = value.getAsDouble();
        if (!Double.isFinite(number) || number < min || number > max) {
            throw new IllegalArgumentException(key + " is outside its supported range");
        }
    }
}
