package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.RowJsonEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonCodec implements AbstractCodec {
    private List<FieldSchema> fieldSchemas;

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        this.fieldSchemas = fieldSchemas;
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return JsonRows.decode(json, fieldSchemas, JsonRows.Decoding.RAW);
    }

    @Override
    public byte[] encode(Object[] objects) {
        String json = RowJsonEncoder.toJson(objects, fieldSchemas);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
