package com.sqlrec.connectors.kafka.calcite;

import com.google.protobuf.StringValue;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.common.utils.SilenceLoggers;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaCalciteTableUnitTest {

    @Mock
    private KafkaProducer<String, byte[]> mockProducer;

    private KafkaCalciteTable table;
    private KafkaConfig config;
    private String configKey;

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

        configKey = KafkaCalciteTable.getProducerConfigKeyForTest(config);
        KafkaCalciteTable.setKafkaProducerForTest(configKey, mockProducer);
    }

    @AfterEach
    public void tearDown() {
        KafkaCalciteTable.invalidateProducer(configKey);
    }

    @Test
    public void testGetProducerConfigKey() {
        String key = KafkaCalciteTable.getProducerConfigKeyForTest(config);

        assertEquals("localhost:9092|10", key);
    }

    @Test
    public void testGetProducerConfigKeyDifferentLinger() {
        KafkaConfig config2 = new KafkaConfig();
        config2.bootstrapServers = "localhost:9092";
        config2.lingerMs = 20;

        String key2 = KafkaCalciteTable.getProducerConfigKeyForTest(config2);

        assertNotEquals(configKey, key2);
    }

    @Test
    public void testGetKafkaProducerReturnsInjected() {
        KafkaProducer<String, byte[]> producer = KafkaCalciteTable.getKafkaProducer(config);

        assertSame(mockProducer, producer);
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
        assertEquals(
                KafkaCalciteTable.getProducerConfigKeyForTest(config),
                KafkaCalciteTable.getProducerConfigKeyForTest(protobufConfig));
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

        // Same producer config key as setUp, so mockProducer is already injected
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
    public void testInvalidateProducer() {
        KafkaCalciteTable.invalidateProducer(configKey);

        verify(mockProducer, times(1)).close();
    }

    @Test
    public void testCloseAllProducers() {
        KafkaCalciteTable.closeAllProducers();

        verify(mockProducer, times(1)).close();
    }

    @Test
    public void testCloseAllProducersContinuesWhenCloseFails() {
        doThrow(new RuntimeException("close failed")).when(mockProducer).close();

        assertDoesNotThrow(KafkaCalciteTable::closeAllProducers);

        assertTrue(KafkaCalciteTable.kafkaProducerMap.isEmpty());
        verify(mockProducer).close();
    }
}
