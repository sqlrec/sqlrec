package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.kafka.calcite.KafkaCalciteTable;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tag("integration")
public class TestKafkaTable {
    private static final String BOOTSTRAP_SERVERS = SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30021";
    private static final String TEST_TOPIC = "sqlrec_kafka_test_"
            + UUID.randomUUID().toString().replace("-", "");
    private static AdminClient adminClient;

    @BeforeAll
    static void createTestTopic() throws Exception {
        adminClient = AdminClient.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
        adminClient.createTopics(Collections.singletonList(new NewTopic(TEST_TOPIC, 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
    }

    @AfterAll
    static void dropTestTopic() throws Exception {
        if (adminClient == null) {
            return;
        }
        try {
            // Drain asynchronous writes before deleting, so a late send cannot
            // recreate the topic when broker auto-creation is enabled.
            KafkaCalciteTable.closeAllProducers();
            if (adminClient.listTopics().names().get(30, TimeUnit.SECONDS).contains(TEST_TOPIC)) {
                adminClient.deleteTopics(Collections.singletonList(TEST_TOPIC))
                        .all().get(30, TimeUnit.SECONDS);
            }
        } finally {
            adminClient.close();
        }
    }

    @Test
    public void testKafkaTable() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tableMap = new HashMap<>();
                tableMap.put("t1", getKafkaTable());
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        new SqlTestCase("insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null, """
                LogicalTableModify(table=[[default, t1]], operation=[INSERT], flattened=[false])
                  LogicalValues(tuples=[[{ 1, 'Alice1', 1 }]])""", """
                SqlrecEnumerableTableModify(table=[[default, t1]], operation=[INSERT], flattened=[false])
                  EnumerableValues(tuples=[[{ 1, 'Alice1', 1 }]])""", null).test(schema);
    }

    public static Table getKafkaTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.bootstrapServers = BOOTSTRAP_SERVERS;
        kafkaConfig.format = "json";
        kafkaConfig.topic = TEST_TOPIC;
        kafkaConfig.fieldSchemas = fieldSchemas;
        kafkaConfig.lingerMs = 5000;

        return new KafkaCalciteTable(kafkaConfig);
    }
}
