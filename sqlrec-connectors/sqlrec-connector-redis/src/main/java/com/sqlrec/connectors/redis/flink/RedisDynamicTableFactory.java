package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.common.schema.ConnectorTableMetadata;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        RedisConfig redisConfig = getRedisConfig(context);
        return new RedisDynamicTableSink(redisConfig, context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        RedisConfig redisConfig = getRedisConfig(context);
        return new RedisDynamicTableSource(redisConfig, context.getCatalogTable().getResolvedSchema());
    }

    private RedisConfig getRedisConfig(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
        RedisConfig redisConfig = RedisOptions.getRedisConfig(options);
        ConnectorTableMetadata metadata = FlinkSchemaUtils.getConnectorTableMetadata(
                context.getObjectIdentifier().getDatabaseName(),
                context.getObjectIdentifier().getObjectName(), options, tableSchema);
        redisConfig.database = metadata.getDatabase();
        redisConfig.tableName = metadata.getTableName();
        redisConfig.fieldSchemas = metadata.getFieldSchemas();
        redisConfig.primaryKey = metadata.getPrimaryKey();
        redisConfig.primaryKeyIndex = metadata.getPrimaryKeyIndex();
        return redisConfig;
    }

    @Override
    public String factoryIdentifier() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.URL));
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.REDIS_MODE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.DATA_STRUCTURE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.FORMAT));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.PROTOBUF_MESSAGE_CLASS_NAME));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.MAX_LIST_SIZE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.TTL));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.CACHE_TTL));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.MAX_CACHE_SIZE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.BATCH_SIZE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(RedisOptions.FLUSH_INTERVAL));
        return options;
    }
}
