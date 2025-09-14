package com.sqlrec.connectors.redis.codec;

import com.google.gson.*;
import com.sqlrec.common.utils.FieldSchema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class JsonCodec implements AbstractCodec {
    private List<FieldSchema> fieldSchemas;
    private Gson gson;

    @Override
    public void init(List<FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
        gson = new GsonBuilder()
                .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                .create();
    }

    @Override
    public Object[] decode(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        Map<String, Object> dataMap = gson.fromJson(json, Map.class);
        Object[] objects = new Object[fieldSchemas.size()];
        for (int i = 0; i < fieldSchemas.size(); i++) {
            objects[i] = dataMap.get(fieldSchemas.get(i).name);
        }
        return objects;
    }

    @Override
    public byte[] encode(Object[] objects) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < fieldSchemas.size(); i++) {
            jsonObject.add(fieldSchemas.get(i).name, gson.toJsonTree(objects[i]));
        }
        return gson.toJson(jsonObject).getBytes(StandardCharsets.UTF_8);
    }
}
