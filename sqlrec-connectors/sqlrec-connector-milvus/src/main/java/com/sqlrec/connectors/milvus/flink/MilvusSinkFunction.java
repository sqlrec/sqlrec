package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.List;

public class MilvusSinkFunction<IN> extends RichSinkFunction<IN> {
    private List<org.apache.flink.table.types.DataType> dataTypes;
    private MilvusHandler milvusHandler;

    public MilvusSinkFunction(MilvusConfig milvusConfig, ResolvedSchema tableSchema) {
        this.dataTypes = tableSchema.getColumnDataTypes();
        this.milvusHandler = new MilvusHandler(milvusConfig);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();

        Object[] objects = FlinkSchemaUtils.transform(rowData, dataTypes);
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            milvusHandler.add(objects);
        } else if (kind == RowKind.DELETE) {
            milvusHandler.remove(objects);
        }
    }
}