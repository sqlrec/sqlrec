package com.sqlrec.common.utils;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/** General JSON serialization; schema-aware row formats live in JsonRows and RowJsonEncoder. */
public final class JsonUtils {
    private JsonUtils() {
    }

    private static final Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    public static Gson getGson() {
        return gson;
    }

    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) throws JsonSyntaxException {
        return gson.fromJson(json, classOfT);
    }

    public static List<String> parseStringList(String json) {
        return gson.fromJson(json, new TypeToken<List<String>>() {
        }.getType());
    }

    public static Map<String, Object> parseJsonToMap(String json) {
        return gson.fromJson(json, Map.class);
    }
}
