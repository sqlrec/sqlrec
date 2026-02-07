package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonCodec implements AbstractCodec {
    private List<FieldSchema> fieldSchemas;

    @Override
    public void init(List<FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
    }

    @Override
    public Object[] decode(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return JsonUtils.fromJson(json, fieldSchemas);
    }

    @Override
    public byte[] encode(Object[] objects) {
        String json = JsonUtils.toJson(objects, fieldSchemas);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
