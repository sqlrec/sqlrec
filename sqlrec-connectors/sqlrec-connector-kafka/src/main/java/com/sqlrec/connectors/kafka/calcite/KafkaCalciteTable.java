package com.sqlrec.connectors.kafka.calcite;

import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.connectors.kafka.config.KafkaConfig;
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

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaCalciteTable extends SqlRecTable implements ModifiableTable {
    private static Map<String, KafkaProducer<String, String>> kafkaProducerMap = new ConcurrentHashMap<>();
    private final KafkaConfig kafkaConfig;

    public KafkaCalciteTable(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, kafkaConfig.fieldSchemas);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new kafkaCollection(kafkaConfig);
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
        return Schemas.tableExpression(schema, getElementType(),
                tableName, clazz);
    }

    public static KafkaProducer<String, String> getKafkaProducer(KafkaConfig kafkaConfig) {
        if (kafkaProducerMap.containsKey(kafkaConfig.bootstrapServers)) {
            return kafkaProducerMap.get(kafkaConfig.bootstrapServers);
        }
        return openKafkaProducer(kafkaConfig);
    }

    private static synchronized KafkaProducer<String, String> openKafkaProducer(KafkaConfig kafkaConfig) {
        if (kafkaProducerMap.containsKey(kafkaConfig.bootstrapServers)) {
            return kafkaProducerMap.get(kafkaConfig.bootstrapServers);
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", 5000);   //todo read table config

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        kafkaProducerMap.put(kafkaConfig.bootstrapServers, producer);
        return producer;
    }

    public static class kafkaCollection implements Collection<Object[]> {
        private int size = 0;
        private KafkaConfig kafkaConfig;

        public kafkaCollection(KafkaConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Object[]> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Object[] objects) {
            size += 1;
            String msg = JsonUtils.toJson(objects, kafkaConfig.fieldSchemas);
            KafkaProducer<String, String> producer = getKafkaProducer(kafkaConfig);
            producer.send(new ProducerRecord<>(kafkaConfig.topic, msg));
            return true;
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends Object[]> c) {
            for (Object[] objects : c) {
                add(objects);
            }
            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }
}
