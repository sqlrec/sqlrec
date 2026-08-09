package com.sqlrec.connectors.kafka.calcite;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaCalciteTableUnitTest {

    @Mock
    private KafkaProducer<String, String> mockProducer;

    private KafkaCalciteTable table;
    private KafkaConfig config;
    private String configKey;

    @BeforeEach
    public void setUp() {
        config = new KafkaConfig();
        config.bootstrapServers = "localhost:9092";
        config.topic = "test_topic";
        config.keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
        config.valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
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

        assertEquals("localhost:9092|org.apache.kafka.common.serialization.StringSerializer|"
                + "org.apache.kafka.common.serialization.StringSerializer|10", key);
    }

    @Test
    public void testGetProducerConfigKeyDifferentLinger() {
        KafkaConfig config2 = new KafkaConfig();
        config2.bootstrapServers = "localhost:9092";
        config2.keySerializer = config.keySerializer;
        config2.valueSerializer = config.valueSerializer;
        config2.lingerMs = 20;

        String key2 = KafkaCalciteTable.getProducerConfigKeyForTest(config2);

        assertNotEquals(configKey, key2);
    }

    @Test
    public void testGetKafkaProducerReturnsInjected() {
        KafkaProducer<String, String> producer = KafkaCalciteTable.getKafkaProducer(config);

        assertSame(mockProducer, producer);
    }

    @Test
    public void testAddImpl() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        Object[] row = new Object[]{1, "alice"};
        boolean result = collection.add(row);

        assertTrue(result);

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(1)).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();
        assertEquals("test_topic", record.topic());
        assertNotNull(record.value());
        assertTrue(record.value().contains("\"id\""));
        assertTrue(record.value().contains("\"name\""));
        assertTrue(record.value().contains("alice"));
    }

    @Test
    public void testAddImplMultipleRows() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        collection.add(new Object[]{1, "alice"});
        collection.add(new Object[]{2, "bob"});

        verify(mockProducer, times(2)).send(any());
    }

    @Test
    public void testRemoveImplThrows() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        assertThrows(UnsupportedOperationException.class,
                () -> collection.remove(new Object[]{1, "alice"}));
    }

    @Test
    public void testAddImplWithProducerFailure() {
        KafkaCalciteTable.KafkaCollection collection =
                new KafkaCalciteTable.KafkaCollection(table, config);

        when(mockProducer.send(any())).thenThrow(new RuntimeException("send failed"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> collection.add(new Object[]{1, "test"}));
        assertTrue(ex.getMessage().contains("send failed"));
    }

    @Test
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
        multiTypeConfig.keySerializer = config.keySerializer;
        multiTypeConfig.valueSerializer = config.valueSerializer;
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

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(1)).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();
        assertEquals("test_topic", record.topic());
        assertNotNull(record.value());
        assertTrue(record.value().contains("42"));
        assertTrue(record.value().contains("hello"));
        assertTrue(record.value().contains("3.14"));
        assertTrue(record.value().contains("true"));
    }

    @Test
    public void testInvalidateProducer() {
        KafkaCalciteTable.invalidateProducer(configKey);

        verify(mockProducer, times(1)).close();
    }
}
