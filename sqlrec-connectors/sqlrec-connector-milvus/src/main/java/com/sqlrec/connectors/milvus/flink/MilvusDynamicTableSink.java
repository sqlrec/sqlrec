package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.connectors.milvus.config.MilvusConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class MilvusDynamicTableSink implements DynamicTableSink {
    private MilvusConfig milvusConfig;
    private ResolvedSchema tableSchema;

    public MilvusDynamicTableSink(MilvusConfig milvusConfig, ResolvedSchema tableSchema) {
        this.milvusConfig = milvusConfig;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                new MilvusSinkFunction<>(milvusConfig, tableSchema)
        );
    }

    @Override
    public DynamicTableSink copy() {
        return new MilvusDynamicTableSink(milvusConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "milvus table sink";
    }
}