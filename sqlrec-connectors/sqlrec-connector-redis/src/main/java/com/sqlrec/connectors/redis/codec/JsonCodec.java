package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonRows;
import com.sqlrec.common.utils.RowJsonEncoder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.nio.charset.StandardCharsets;
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
        String json = RowJsonEncoder.toJson(objects, fieldSchemas);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
