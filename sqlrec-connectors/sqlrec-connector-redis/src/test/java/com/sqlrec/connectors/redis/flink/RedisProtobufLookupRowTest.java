package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.codec.ProtobufCodec;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisProtobufLookupRowTest {
    @Test
    void convertsDecodedCollectionsAndNestedMessageToFlinkInternalTypes() {
        List<FieldSchema> fields = Arrays.asList(
                new FieldSchema("string_value", "VARCHAR"),
                new FieldSchema("repeated_string", "ARRAY<VARCHAR>"),
                new FieldSchema("status_by_name", "MAP<VARCHAR, VARCHAR>"),
                new FieldSchema("nested", "ROW"));
        ProtobufCodec codec = new ProtobufCodec("com.sqlrec.connectors.redis.proto.RedisAllTypes");
        codec.init(fields, 0);

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("id", 7L);
        nested.put("name", "child");
        nested.put("enabled", true);
        Object[] decoded = codec.decode(codec.encode(new Object[]{
                "key", Arrays.asList("red", "blue"),
                Collections.singletonMap("primary", "ACTIVE"), nested
        }), "key");

        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("string_value", DataTypes.STRING()),
                Column.physical("repeated_string", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("status_by_name",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                Column.physical("nested", DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()))));

        GenericRowData row = FlinkSchemaUtils.toRowData(decoded, schema.getColumnDataTypes());

        assertEquals(StringData.fromString("key"), row.getString(0));
        assertEquals(StringData.fromString("red"), row.getArray(1).getString(0));
        assertEquals(StringData.fromString("blue"), row.getArray(1).getString(1));
        MapData statuses = row.getMap(2);
        assertEquals(StringData.fromString("primary"), statuses.keyArray().getString(0));
        assertEquals(StringData.fromString("ACTIVE"), statuses.valueArray().getString(0));
        RowData child = row.getRow(3, 3);
        assertEquals(7L, child.getLong(0));
        assertEquals(StringData.fromString("child"), child.getString(1));
        assertEquals(true, child.getBoolean(2));
    }
}
