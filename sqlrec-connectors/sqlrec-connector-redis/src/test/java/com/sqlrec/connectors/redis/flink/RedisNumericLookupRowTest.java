package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.codec.JsonCodec;
import com.sqlrec.connectors.redis.codec.StringCodec;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisNumericLookupRowTest {
    @Test
    void jsonNumbersAndArrayElementsCanBeReadAsFlinkTypes() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("count", DataTypes.INT()),
                Column.physical("score", DataTypes.FLOAT()),
                Column.physical("ids", DataTypes.ARRAY(DataTypes.INT())),
                Column.physical("scores", DataTypes.ARRAY(DataTypes.FLOAT())));
        JsonCodec codec = new JsonCodec();
        codec.init(FlinkSchemaUtils.getFieldSchemas(schema), 0);
        Object[] decoded = codec.decode(
                "{\"id\":1,\"count\":2,\"score\":0.25,\"ids\":[3,null],\"scores\":[0.5,null]}"
                        .getBytes(StandardCharsets.UTF_8), "1");

        GenericRowData row = FlinkSchemaUtils.toRowData(decoded, schema.getColumnDataTypes());

        assertEquals(1L, row.getLong(0));
        assertEquals(2, row.getInt(1));
        assertEquals(0.25f, row.getFloat(2));
        assertEquals(3, row.getArray(3).getInt(0));
        assertTrue(row.getArray(3).isNullAt(1));
        assertEquals(0.5f, row.getArray(4).getFloat(0));
        assertTrue(row.getArray(4).isNullAt(1));
    }

    @Test
    void sharedStringCodecFloatValuesCanBeReadAsFlinkFloat() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()), Column.physical("score", DataTypes.FLOAT()));
        StringCodec codec = new StringCodec();
        codec.init(FlinkSchemaUtils.getFieldSchemas(schema), 0);
        Object[] decoded = codec.decode("0.25".getBytes(StandardCharsets.UTF_8), "1");

        GenericRowData row = FlinkSchemaUtils.toRowData(decoded, schema.getColumnDataTypes());

        assertEquals(1L, row.getLong(0));
        assertEquals(0.25f, row.getFloat(1));
    }
}
