package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.ScalarConversions;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class StringCodec implements AbstractCodec {

    private SqlTypeName[] fieldTypes;
    private int primaryKeyIndex;

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        if (fieldSchemas.size() != 2) {
            throw new IllegalArgumentException(
                    "String data structure requires exactly 2 fields: one primary key and one value, but got "
                            + fieldSchemas.size() + " fields");
        }
        this.fieldTypes = new SqlTypeName[]{resolveType(fieldSchemas.get(0)), resolveType(fieldSchemas.get(1))};
        this.primaryKeyIndex = primaryKeyIndex;
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        String value = new String(bytes, StandardCharsets.UTF_8);
        int valueIndex = 1 - primaryKeyIndex;
        Object[] result = new Object[2];
        result[primaryKeyIndex] = ScalarConversions.convert(primaryKey, fieldTypes[primaryKeyIndex]);
        result[valueIndex] = ScalarConversions.convert(value, fieldTypes[valueIndex]);
        return result;
    }

    @Override
    public byte[] encode(Object[] objects) {
        int valueIndex = 1 - primaryKeyIndex;
        return objects[valueIndex].toString().getBytes(StandardCharsets.UTF_8);
    }

    private static SqlTypeName resolveType(FieldSchema field) {
        try {
            return DataTypeUtils.getRelDataType(field.getType()).getSqlTypeName();
        } catch (IllegalArgumentException ignored) {
            // Preserve the string representation for unknown types.
            return null;
        }
    }
}
