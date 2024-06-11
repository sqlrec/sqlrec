package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.config.RedisOptions;
import org.apache.flink.configuration.ConfigOption;
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

        Map<String, String> options = context.getCatalogTable().getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
        RedisConfig redisConfig = RedisOptions.getRedisConfig(options);

        return new RedisDynamicTableSink(redisConfig, tableSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        Map<String, String> options = context.getCatalogTable().getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
        RedisConfig redisConfig = RedisOptions.getRedisConfig(options);

        return new RedisDynamicTableSource(redisConfig, tableSchema);
    }

    @Override
    public String factoryIdentifier() {
        return RedisOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.REDIS_MODE);
        options.add(RedisOptions.DATA_STRUCTURE);
        options.add(RedisOptions.TTL);
        options.add(RedisOptions.CACHE_TTL);
        return options;
    }
}
