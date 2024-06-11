package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.RedisConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

public class RedisDynamicTableSource implements LookupTableSource {
    private RedisConfig redisConfig;
    private ResolvedSchema tableSchema;

    public RedisDynamicTableSource(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        this.tableSchema = tableSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupTableFunction(redisConfig, tableSchema)
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(redisConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "redis table source";
    }
}
