package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.config.MilvusOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MilvusDynamicTableFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        MilvusConfig milvusConfig = getMilvusConfig(context);
        return new MilvusDynamicTableSink(milvusConfig, context.getCatalogTable().getResolvedSchema());
    }

    private MilvusConfig getMilvusConfig(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();
        MilvusConfig milvusConfig = MilvusOptions.getMilvusConfig(options);
        milvusConfig.fieldSchemas = FlinkSchemaUtils.getFieldSchemas(tableSchema);
        milvusConfig.primaryKey = FlinkSchemaUtils.getPrimaryKey(tableSchema);
        milvusConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(
                milvusConfig.fieldSchemas,
                milvusConfig.primaryKey
        );
        return milvusConfig;
    }

    @Override
    public String factoryIdentifier() {
        return MilvusOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.URL));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.COLLECTION));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.TOKEN));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.DATABASE));
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.BATCH_SIZE));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.FLUSH_INTERVAL));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.POOL_MAX_IDLE_PER_KEY));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.POOL_MAX_TOTAL_PER_KEY));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.POOL_MAX_TOTAL));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.POOL_MAX_BLOCK_WAIT_DURATION));
        options.add(FlinkSchemaUtils.toFlinkConfigOption(MilvusOptions.POOL_MIN_EVICTABLE_IDLE_DURATION));
        return options;
    }
}