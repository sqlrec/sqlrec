package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class RedisSinkTableFunction<IN> extends RichSinkFunction<IN> {
    private RedisConfig redisConfig;
    private List<DataType> dataTypes;
    private RedisHandler redisHandler;

    public RedisSinkTableFunction(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        dataTypes = tableSchema.getColumnDataTypes();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisHandler = new RedisHandler(redisConfig);
        redisHandler.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisHandler != null) {
            redisHandler.close();
            redisHandler = null;
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
        if (!(value instanceof RowData)) {
            throw new IllegalArgumentException("Expected RowData but got: " + value.getClass().getName());
        }
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();

        Object[] objects = FlinkSchemaUtils.transform(rowData, dataTypes);
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            redisHandler.insert(objects);
        } else if (kind == RowKind.DELETE) {
            redisHandler.delete(objects);
        }
    }
}
