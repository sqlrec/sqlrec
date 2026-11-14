package com.sqlrec.connectors.kafka.calcite;

import com.google.protobuf.StringValue;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.connectors.kafka.utils.KafkaProducerUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class KafkaCalciteTableUnitTest {

    private KafkaProducer<String, byte[]> mockProducer;
    private MockedConstruction<KafkaProducer> producerConstruction;
    private final List<Properties> producerProperties = new ArrayList<>();

    private KafkaCalciteTable table;
    private KafkaConfig config;

    @BeforeEach
    public void setUp() {
        config = new KafkaConfig();
        config.bootstrapServers = "localhost:9092";
        config.topic = "test_topic";
        config.lingerMs = 10;
        config.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR")
        );

        table = new KafkaCalciteTable(config);
        table.setTableName("test_table");

        producerConstruction = mockConstruction(KafkaProducer.class,
                (producer, context) -> producerProperties.add((Properties) context.arguments().get(0)));
        mockProducer = KafkaCalciteTable.getKafkaProducer(config);
    }

    @AfterEach
    public void tearDown() {
        try {
            KafkaCalciteTable.closeAllProducers();
        } finally {
            producerConstruction.close();
        }
    }

    @Test
    public void testProducerConnectionAndSerializationProperties() {
        Properties properties = producerProperties.get(0);
        assertEquals("localhost:9092", properties.getProperty("bootstrap.servers"));
        assertEquals("10", properties.getProperty("linger.ms"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", properties.getProperty("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", properties.getProperty("value.serializer"));
    }

    @Test
    public void testDifferentLingerCreatesAnotherProducer() {
        KafkaConfig config2 = new KafkaConfig();
        config2.bootstrapServers = "localhost:9092";
        config2.lingerMs = 20;

        assertNotSame(mockProducer, KafkaCalciteTable.getKafkaProducer(config2));
    }

    @Test
    public void testTableAndUtilityReuseTheSameProducer() {
        KafkaProducer<String, byte[]> producer = KafkaCalciteTable.getKafkaProducer(config);

        assertSame(mockProducer, producer);
        assertSame(producer, KafkaProducerUtils.getOrCreate(config));
        assertEquals(1, producerConstruction.constructed().size());
    }

    @Test
    public void testJsonAndProtobufTablesReuseProducer() {
        KafkaConfig protobufConfig = new KafkaConfig();
        protobufConfig.bootstrapServers = config.bootstrapServers;
        protobufConfig.lingerMs = config.lingerMs;
        protobufConfig.format = "protobuf";

        assertSame(
                KafkaCalciteTable.getKafkaProducer(config),
                KafkaCalciteTable.getKafkaProducer(protobufConfig));
        assertEquals(1, producerConstruction.constructed().size());
    }

    @Test
    public void testAddImpl() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        Object[] row = new Object[]{1, "alice"};
        boolean result = collection.add(row);

        assertTrue(result);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(1)).send(captor.capture(), any());

        ProducerRecord<String, byte[]> record = captor.getValue();
        assertEquals("test_topic", record.topic());
        assertNotNull(record.value());
        String json = new String(record.value(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"id\""));
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("alice"));
    }

    @Test
    public void testAddImplMultipleRows() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        collection.add(new Object[]{1, "alice"});
        collection.add(new Object[]{2, "bob"});

        verify(mockProducer, times(2)).send(any(), any());
    }

    @Test
    public void testAddImplWithProtobufFormat() throws Exception {
        KafkaConfig protobufConfig = new KafkaConfig();
        protobufConfig.bootstrapServers = config.bootstrapServers;
        protobufConfig.topic = config.topic;
        protobufConfig.format = "protobuf";
        protobufConfig.protobufMessageClassName = "com.google.protobuf.StringValue";
        protobufConfig.lingerMs = config.lingerMs;
        protobufConfig.fieldSchemas = Arrays.asList(new FieldSchema("value", "VARCHAR"));

        KafkaCalciteTable protobufTable = new KafkaCalciteTable(protobufConfig);
        protobufTable.setTableName("protobuf_table");
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(protobufTable, protobufConfig);
        collection.add(new Object[]{"hello"});

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());
        assertEquals("hello", StringValue.parseFrom(captor.getValue().value()).getValue());
    }

    @Test
    public void testProtobufTableRejectsInvalidMessageClass() {
        KafkaConfig protobufConfig = new KafkaConfig();
        protobufConfig.format = "protobuf";
        protobufConfig.protobufMessageClassName = "example.missing.Message";

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaCalciteTable(protobufConfig));
    }

    @Test
    @SilenceLoggers(SqlRecCollection.class)
    public void testProtobufAddRejectsIncompatibleRowValue() {
        KafkaConfig protobufConfig = new KafkaConfig();
        protobufConfig.bootstrapServers = config.bootstrapServers;
        protobufConfig.topic = config.topic;
        protobufConfig.format = "protobuf";
        protobufConfig.protobufMessageClassName = "com.google.protobuf.Int32Value";
        protobufConfig.lingerMs = config.lingerMs;
        protobufConfig.fieldSchemas = Arrays.asList(new FieldSchema("value", "INTEGER"));
        KafkaCalciteTable protobufTable = new KafkaCalciteTable(protobufConfig);
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(protobufTable, protobufConfig);

        assertThrows(IllegalArgumentException.class,
                () -> collection.add(new Object[]{"not-a-number"}));
        verify(mockProducer, never()).send(any(), any());
    }

    @Test
    @SilenceLoggers(SqlRecCollection.class)
    public void testRemoveImplThrows() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        assertThrows(UnsupportedOperationException.class,
                () -> collection.remove(new Object[]{1, "alice"}));
    }

    @Test
    @SilenceLoggers(SqlRecCollection.class)
    public void testAddImplWithProducerFailure() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        when(mockProducer.send(any(), any())).thenThrow(new RuntimeException("send failed"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> collection.add(new Object[]{1, "test"}));
        assertTrue(ex.getMessage().contains("send failed"));
    }

    @Test
    @SilenceLoggers(KafkaCalciteTable.KafkaCollection.class)
    public void testAsynchronousProducerFailureCallback() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        collection.add(new Object[]{1, "test"});

        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(mockProducer).send(any(), callbackCaptor.capture());
        assertDoesNotThrow(() -> callbackCaptor.getValue().onCompletion(
                null, new RuntimeException("async send failed")));
    }

    @Test
    @SilenceLoggers(SqlRecCollection.class)
    public void testAddImplWithNullRow() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        assertThrows(Exception.class, () -> collection.add(null));
    }

    @Test
    public void testAddImplWithDifferentTypes() {
        KafkaConfig multiTypeConfig = new KafkaConfig();
        multiTypeConfig.bootstrapServers = config.bootstrapServers;
        multiTypeConfig.topic = config.topic;
        multiTypeConfig.lingerMs = config.lingerMs;
        multiTypeConfig.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("price", "FLOAT"),
                new FieldSchema("active", "BOOLEAN")
        );

        KafkaCalciteTable multiTable = new KafkaCalciteTable(multiTypeConfig);
        multiTable.setTableName("multi_table");

        // The same connection settings reuse the producer created in setUp.
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(multiTable, multiTypeConfig);

        collection.add(new Object[]{42, "hello", 3.14, true});

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(1)).send(captor.capture(), any());

        ProducerRecord<String, byte[]> record = captor.getValue();
        assertEquals("test_topic", record.topic());
        assertNotNull(record.value());
        String json = new String(record.value(), StandardCharsets.UTF_8);
        assertTrue(json.contains("42"));
        assertTrue(json.contains("hello"));
        assertTrue(json.contains("3.14"));
        assertTrue(json.contains("true"));
    }

    @Test
    public void testClosedProducerIsRecreatedOnNextUse() {
        KafkaCalciteTable.closeAllProducers();

        verify(mockProducer, times(1)).close();
        assertNotSame(mockProducer, KafkaCalciteTable.getKafkaProducer(config));
    }

    @Test
    public void testCloseAllProducers() {
        KafkaCalciteTable.closeAllProducers();

        verify(mockProducer, times(1)).close();
    }

    @Test
    public void testCloseAllProducersContinuesWhenCloseFails() {
        KafkaConfig otherConfig = new KafkaConfig();
        otherConfig.bootstrapServers = "localhost:9093";
        otherConfig.lingerMs = config.lingerMs;
        KafkaProducer<String, byte[]> otherProducer = KafkaProducerUtils.getOrCreate(otherConfig);
        doThrow(new RuntimeException("close failed")).when(mockProducer).close();

        assertDoesNotThrow(KafkaCalciteTable::closeAllProducers);

        verify(mockProducer).close();
        verify(otherProducer).close();
        assertNotSame(mockProducer, KafkaCalciteTable.getKafkaProducer(config));
    }
}
