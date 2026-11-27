package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.codec.ProtobufCodec;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisProtobufLookupRowTest {
    @Test
    void convertsDecodedCollectionsToFlinkInternalTypes() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("string_value", DataTypes.STRING()),
                Column.physical("repeated_string", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("status_by_name",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        ProtobufCodec codec = new ProtobufCodec("com.sqlrec.connectors.redis.proto.RedisAllTypes");
        codec.init(FlinkSchemaUtils.getFieldSchemas(schema), 0);

        Object[] decoded = codec.decode(codec.encode(new Object[]{
                "key", Arrays.asList("red", "blue"),
                Collections.singletonMap("primary", "ACTIVE")
        }), "key");

        GenericRowData row = FlinkSchemaUtils.toRowData(decoded, schema.getColumnDataTypes());

        assertEquals(StringData.fromString("key"), row.getString(0));
        assertEquals(StringData.fromString("red"), row.getArray(1).getString(0));
        assertEquals(StringData.fromString("blue"), row.getArray(1).getString(1));
        MapData statuses = row.getMap(2);
        assertEquals(StringData.fromString("primary"), statuses.keyArray().getString(0));
        assertEquals(StringData.fromString("ACTIVE"), statuses.valueArray().getString(0));
    }
}
