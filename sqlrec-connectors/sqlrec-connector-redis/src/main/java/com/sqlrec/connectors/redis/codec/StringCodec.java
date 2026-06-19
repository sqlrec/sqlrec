package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class StringCodec implements AbstractCodec {

    private List<FieldSchema> fieldSchemas;
    private int primaryKeyIndex;

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        if (fieldSchemas.size() != 2) {
            throw new IllegalArgumentException(
                    "String data structure requires exactly 2 fields: one primary key and one value, but got "
                            + fieldSchemas.size() + " fields");
        }
        this.fieldSchemas = fieldSchemas;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        String value = new String(bytes, StandardCharsets.UTF_8);
        int valueIndex = 1 - primaryKeyIndex;
        Object[] result = new Object[2];
        result[primaryKeyIndex] = DataTypeUtils.parseStringAsType(primaryKey, fieldSchemas.get(primaryKeyIndex).getType());
        result[valueIndex] = DataTypeUtils.parseStringAsType(value, fieldSchemas.get(valueIndex).getType());
        return result;
    }

    @Override
    public byte[] encode(Object[] objects) {
        int valueIndex = 1 - primaryKeyIndex;
        return objects[valueIndex].toString().getBytes(StandardCharsets.UTF_8);
    }
}
