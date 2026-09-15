package com.sqlrec.model.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ModelConfigUtils {
    private static final Gson PRETTY_GSON = new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .create();

    private ModelConfigUtils() {
    }

    public static Map<String, String> mergeParams(
            Map<String, String> base,
            Map<String, String> overrides
    ) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        if (overrides != null) {
            merged.putAll(overrides);
        }
        return merged;
    }

    public static String toPrettyJson(JsonElement json) {
        return PRETTY_GSON.toJson(json) + "\n";
    }
}
