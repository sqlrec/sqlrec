package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.RowJsonEncoder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class JsonCodec implements AbstractCodec {
    private List<FieldSchema> fieldSchemas;
    private RelDataType schema;

    @Override
    public void init(List<FieldSchema> fieldSchemas, int primaryKeyIndex) {
        this.schema = DataTypeUtils.getRelDataType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), fieldSchemas);
        this.fieldSchemas = fieldSchemas;
    }

    @Override
    public Object[] decode(byte[] bytes, String primaryKey) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return JsonRows.decode(json, schema);
    }

    @Override
    public byte[] encode(Object[] objects) {
        Object[] values = objects;
        for (int i = 0; i < fieldSchemas.size(); i++) {
            Object value = objects[i];
            String isoText;
            if (value instanceof Date) {
                isoText = ((Date) value).toLocalDate().toString();
            } else if (value instanceof Timestamp) {
                isoText = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime());
            } else {
                continue;
            }
            if (values == objects) {
                values = Arrays.copyOf(objects, objects.length, Object[].class);
            }
            values[i] = isoText;
        }
        String json = RowJsonEncoder.toJson(values, fieldSchemas);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
