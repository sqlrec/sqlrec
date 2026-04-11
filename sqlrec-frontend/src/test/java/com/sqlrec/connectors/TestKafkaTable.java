package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.kafka.calcite.KafkaCalciteTable;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag("integration")
public class TestKafkaTable {
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
        HmsSchema.setGlobalSchema(schema);

        new SqlTestCase("insert into t1 (ID, NAME, CNT) values (1, 'Alice1', 1)", null, """
                LogicalTableModify(table=[[default, t1]], operation=[INSERT], flattened=[false])
                  LogicalValues(tuples=[[{ 1, 'Alice1', 1 }]])""", """
                SqlRecEnumerableTableModify(table=[[default, t1]], operation=[INSERT], flattened=[false])
                  EnumerableValues(tuples=[[{ 1, 'Alice1', 1 }]])""", null).test(schema);
    }

    public static Table getKafkaTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("ID", "INTEGER"));
        fieldSchemas.add(new FieldSchema("NAME", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("CNT", "INTEGER"));

        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.bootstrapServers = SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":32092";
        kafkaConfig.format = "json";
        kafkaConfig.topic = "t1";
        kafkaConfig.fieldSchemas = fieldSchemas;

        return new KafkaCalciteTable(kafkaConfig);
    }
}
