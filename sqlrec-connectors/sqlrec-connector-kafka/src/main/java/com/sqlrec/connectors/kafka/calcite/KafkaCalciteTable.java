package com.sqlrec.connectors.kafka.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.ProtobufRowCodec;
import com.sqlrec.common.utils.RowJsonEncoder;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
import com.sqlrec.connectors.kafka.utils.KafkaProducerUtils;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KafkaCalciteTable extends SqlRecTable implements ModifiableTable {
    private final KafkaConfig kafkaConfig;
    private final ProtobufRowCodec protobufRowCodec;

    public KafkaCalciteTable(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.protobufRowCodec = "protobuf".equals(kafkaConfig.format)
                ? new ProtobufRowCodec(kafkaConfig.protobufMessageClassName)
                : null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, kafkaConfig.fieldSchemas);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new KafkaCollection(this, kafkaConfig);
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public static KafkaProducer<String, byte[]> getKafkaProducer(KafkaConfig kafkaConfig) {
        return KafkaProducerUtils.getOrCreate(kafkaConfig);
    }

    /** Close every cached Kafka producer and clear the cache. */
    public static synchronized void closeAllProducers() {
        KafkaProducerUtils.closeAll();
    }

    public static class KafkaCollection extends SqlRecCollection {
        private static final Logger logger = LoggerFactory.getLogger(KafkaCollection.class);

        private final KafkaCalciteTable table;
        private final KafkaConfig kafkaConfig;

        public KafkaCollection(KafkaCalciteTable table, KafkaConfig kafkaConfig) {
            super(table.getTableName());
            this.table = table;
            this.kafkaConfig = kafkaConfig;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            byte[] msg = table.protobufRowCodec != null
                    ? table.protobufRowCodec.encode(objects, kafkaConfig.fieldSchemas)
                    : RowJsonEncoder.toJson(objects, kafkaConfig.fieldSchemas)
                            .getBytes(StandardCharsets.UTF_8);
            KafkaProducer<String, byte[]> producer = getKafkaProducer(kafkaConfig);
            // Kafka writes are intentionally fire-and-forget. The callback reports asynchronous
            // send failures, while the collection operation returns after enqueueing the record.
            producer.send(new ProducerRecord<>(kafkaConfig.topic, msg), (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send record to kafka topic {}: {}",
                            kafkaConfig.topic, exception.getMessage(), exception);
                }
            });
            return true;
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            throw new UnsupportedOperationException();
        }
    }
}
