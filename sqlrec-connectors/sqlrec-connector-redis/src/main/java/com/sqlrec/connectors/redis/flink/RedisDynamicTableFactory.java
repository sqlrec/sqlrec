package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.common.utils.HiveTableUtils;
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
        redisConfig.database = context.getObjectIdentifier().getDatabaseName();
        redisConfig.tableName = context.getObjectIdentifier().getObjectName();
        redisConfig.fieldSchemas = FlinkSchemaUtils.getFieldSchemas(tableSchema);
        redisConfig.primaryKey = FlinkSchemaUtils.getPrimaryKey(tableSchema);
        redisConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(
                redisConfig.fieldSchemas,
                redisConfig.primaryKey
        );
        return redisConfig;
    }

    @Override
    public String factoryIdentifier() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(toFlinkConfigOption(RedisOptions.URL));
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(toFlinkConfigOption(RedisOptions.REDIS_MODE));
        options.add(toFlinkConfigOption(RedisOptions.DATA_STRUCTURE));
        options.add(toFlinkConfigOption(RedisOptions.TTL));
        options.add(toFlinkConfigOption(RedisOptions.CACHE_TTL));
        return options;
    }

    public static <T> ConfigOption<T> toFlinkConfigOption(com.sqlrec.common.config.ConfigOption<T> configOption) {
        return ConfigOptions.key(configOption.getKey()).defaultValue(configOption.getDefaultValue());
    }
}
