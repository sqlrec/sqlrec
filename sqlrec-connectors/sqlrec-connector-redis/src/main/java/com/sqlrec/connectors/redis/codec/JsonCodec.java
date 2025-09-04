package com.sqlrec.connectors.redis.codec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.sqlrec.common.utils.FieldSchema;

import java.nio.charset.StandardCharsets;
import java.util.List;

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
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), Object[].class);
    }

    @Override
    public byte[] encode(Object[] objects) {
        return gson.toJson(objects).getBytes(StandardCharsets.UTF_8);
    }
}
