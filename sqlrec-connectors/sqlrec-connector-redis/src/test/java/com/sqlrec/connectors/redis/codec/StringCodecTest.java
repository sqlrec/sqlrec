package com.sqlrec.connectors.redis.codec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StringCodecTest {

    private List<FieldSchema> twoFields(String firstType, String secondType) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", firstType));
        fieldSchemas.add(new FieldSchema("value", secondType));
        return fieldSchemas;
    }

    @Test
    void testInitRequiresExactlyTwoFields() {
        StringCodec codec = new StringCodec();
        assertThrows(IllegalArgumentException.class,
                () -> codec.init(twoFields("string", "string").subList(0, 1), 0));

        List<FieldSchema> threeFields = new ArrayList<>(twoFields("string", "string"));
        threeFields.add(new FieldSchema("extra", "string"));
        assertThrows(IllegalArgumentException.class, () -> codec.init(threeFields, 0));

        // exactly two fields is accepted
        codec.init(twoFields("string", "string"), 0);
    }

    @Test
    void testEncodeValueColumnWhenPrimaryKeyIsFirst() {
        StringCodec codec = new StringCodec();
        codec.init(twoFields("string", "string"), 0);

        // primaryKeyIndex=0 -> the value column is index 1
        byte[] encoded = codec.encode(new Object[]{"rowKey", "hello"});
        assertEquals("hello", new String(encoded, StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeValueColumnWhenPrimaryKeyIsSecond() {
        StringCodec codec = new StringCodec();
        codec.init(twoFields("string", "string"), 1);

        // primaryKeyIndex=1 -> the value column is index 0
        byte[] encoded = codec.encode(new Object[]{"hello", "rowKey"});
        assertEquals("hello", new String(encoded, StandardCharsets.UTF_8));
    }

    @Test
    void testDecodeRoundTrip() {
        StringCodec codec = new StringCodec();
        codec.init(twoFields("string", "string"), 0);

        byte[] encoded = codec.encode(new Object[]{"rowKey", "hello"});
        Object[] decoded = codec.decode(encoded, "rowKey");

        // decode restores the full row: [primaryKey, value]
        assertArrayEquals(new Object[]{"rowKey", "hello"}, decoded);
    }

    @Test
    void testDecodeWithTypedColumns() {
        StringCodec codec = new StringCodec();
        // int primary key, bigint value -> parsed to their declared types
        codec.init(Arrays.asList(
                new FieldSchema("id", "int"),
                new FieldSchema("count", "bigint")), 0);

        Object[] decoded = codec.decode("42".getBytes(StandardCharsets.UTF_8), "7");

        assertEquals(7, decoded[0]);
        assertEquals(42L, decoded[1]);
    }

    @Test
    void decodesNonNullNumericColumnsFromHiveFallback() {
        Table table = new Table();
        table.setSd(new StorageDescriptor());
        table.putToParameters("flink.schema.0.name", "count");
        table.putToParameters("flink.schema.0.data-type", "INT NOT NULL");
        table.putToParameters("flink.schema.1.name", "id");
        table.putToParameters("flink.schema.1.data-type", "BIGINT NOT NULL");

        StringCodec codec = new StringCodec();
        codec.init(HiveTableUtils.parse(table), 1);

        assertArrayEquals(new Object[]{42, 7L}, codec.decode("42".getBytes(StandardCharsets.UTF_8), "7"));
    }

    @Test
    void decodesDecimalWithPrecisionAndNullability() {
        StringCodec codec = new StringCodec();
        codec.init(twoFields("string", "decimal(12, 2) not null"), 0);

        assertArrayEquals(new Object[]{"rowKey", new BigDecimal("42.50")},
                codec.decode("42.50".getBytes(StandardCharsets.UTF_8), "rowKey"));
    }

    @Test
    void leavesUnknownAndTemporalTypesAsStrings() {
        StringCodec codec = new StringCodec();
        codec.init(twoFields("custom_type", "TIMESTAMP(3) NOT NULL"), 0);

        String timestamp = "2026-11-21 12:30:00.123";
        assertArrayEquals(new Object[]{"rowKey", timestamp},
                codec.decode(timestamp.getBytes(StandardCharsets.UTF_8), "rowKey"));
    }
}
