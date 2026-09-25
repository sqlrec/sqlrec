package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.codec.ProtobufCodec;
import com.sqlrec.connectors.redis.config.RedisConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisDynamicTableFactoryTest {

    @Test
    void exposesProtobufOptionsToFlinkValidation() {
        RedisDynamicTableFactory factory = new RedisDynamicTableFactory();

        Set<String> optionKeys = factory.optionalOptions().stream()
                .map(option -> option.key())
                .collect(Collectors.toSet());

        assertTrue(optionKeys.contains("format"));
        assertTrue(optionKeys.contains("protobuf.message-class-name"));
    }

    @Test
    void propagatesProtobufOptionsToSourceAndSink() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "redis");
        options.put("url", "redis://localhost:6379");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.google.protobuf.Field");
        DynamicTableFactory.Context context = context(options);
        RedisDynamicTableFactory factory = new RedisDynamicTableFactory();

        DynamicTableSink sink = factory.createDynamicTableSink(context);
        DynamicTableSource source = factory.createDynamicTableSource(context);

        assertProtobufConfig(readConfig(sink));
        assertProtobufConfig(readConfig(source));
    }

    @Test
    void preservesMapValueTypeForProtobufEnumDecoding() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "redis");
        options.put("url", "redis://localhost:6379");
        options.put("format", "protobuf");
        options.put("protobuf.message-class-name", "com.sqlrec.connectors.redis.proto.RedisAllTypes");
        ResolvedSchema schema = new ResolvedSchema(
                Arrays.asList(
                        Column.physical("string_value", DataTypes.STRING()),
                        Column.physical("status_by_name",
                                DataTypes.MAP(DataTypes.STRING().notNull(),
                                        DataTypes.STRING().notNull()).notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("string_value")));
        RedisConfig config = readConfig(new RedisDynamicTableFactory()
                .createDynamicTableSource(context(options, schema)));

        ProtobufCodec codec = new ProtobufCodec(config.protobufMessageClassName);
        codec.init(config.fieldSchemas, config.primaryKeyIndex);
        byte[] encoded = codec.encode(new Object[]{
                "key", Collections.singletonMap("primary", "ACTIVE")});
        Map<?, ?> decoded = (Map<?, ?>) codec.decode(encoded, "key")[1];
        assertEquals("ACTIVE", decoded.get("primary"));
    }

    private static DynamicTableFactory.Context context(Map<String, String> options) {
        ResolvedSchema schema = new ResolvedSchema(
                Arrays.asList(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("json_name", DataTypes.STRING())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("name")));
        return context(options, schema);
    }

    private static DynamicTableFactory.Context context(
            Map<String, String> options, ResolvedSchema schema) {
        ResolvedCatalogTable catalogTable = mock(ResolvedCatalogTable.class);
        when(catalogTable.getOptions()).thenReturn(options);
        when(catalogTable.getResolvedSchema()).thenReturn(schema);

        DynamicTableFactory.Context context = mock(DynamicTableFactory.Context.class);
        when(context.getCatalogTable()).thenReturn(catalogTable);
        when(context.getObjectIdentifier()).thenReturn(
                ObjectIdentifier.of("catalog", "database", "protobuf_table"));
        when(context.getConfiguration()).thenReturn(new Configuration());
        when(context.getClassLoader()).thenReturn(
                RedisDynamicTableFactoryTest.class.getClassLoader());
        when(context.getEnrichmentOptions()).thenReturn(Collections.emptyMap());
        return context;
    }

    private static RedisConfig readConfig(Object table) throws Exception {
        Field field = table.getClass().getDeclaredField("redisConfig");
        field.setAccessible(true);
        return (RedisConfig) field.get(table);
    }

    private static void assertProtobufConfig(RedisConfig config) {
        assertEquals("protobuf", config.format);
        assertEquals("com.google.protobuf.Field", config.protobufMessageClassName);
        assertEquals("database", config.database);
        assertEquals("protobuf_table", config.tableName);
        assertEquals("name", config.primaryKey);
        assertEquals(0, config.primaryKeyIndex);
    }
}
