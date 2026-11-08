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
    void sharedJsonDecodingHandlesDecimalAndNestedFlinkSchema() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("amount", DataTypes.DECIMAL(8, 2)),
                Column.physical("children", DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("Score", DataTypes.FLOAT()), DataTypes.FIELD("Count", DataTypes.INT()))))));
        JsonCodec codec = new JsonCodec();
        codec.init(FlinkSchemaUtils.getFieldSchemas(schema), 0);
        Object[] decoded = codec.decode(("{\"id\":\"1\",\"amount\":\"12.34\","
                + "\"children\":{\"7\":[{\"Score\":\"0.25\",\"Count\":\"bad\"},null]}}")
                .getBytes(StandardCharsets.UTF_8), "1");
        GenericRowData row = FlinkSchemaUtils.toRowData(decoded, schema.getColumnDataTypes());
        assertEquals(1L, row.getLong(0));
        assertEquals(new java.math.BigDecimal("12.34"), row.getDecimal(1, 8, 2).toBigDecimal());
        assertEquals(7, row.getMap(2).keyArray().getInt(0));
        var child = row.getMap(2).valueArray().getArray(0).getRow(0, 2);
        assertEquals(0.25f, child.getFloat(0));
        assertTrue(child.isNullAt(1));
        assertTrue(row.getMap(2).valueArray().getArray(0).isNullAt(1));
    }

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
                "{\"id\":1e0,\"count\":2.0,\"score\":0.25,\"ids\":[3e0,null],\"scores\":[0.5,null]}"
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
