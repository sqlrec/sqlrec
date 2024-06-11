package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.FieldSchema;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class RedisSinkTableFunction<IN> extends RichSinkFunction<IN> {
    private RedisConfig redisConfig;
    private ResolvedSchema tableSchema;
    private List<FieldSchema> fieldSchemas;
    List<DataType> dataTypes;

    private RedisHandler redisHandler;

    public RedisSinkTableFunction(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        this.tableSchema = tableSchema;
        dataTypes = tableSchema.getColumnDataTypes();
        fieldSchemas = FieldSchema.parse(tableSchema);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisHandler = new RedisHandler(redisConfig, fieldSchemas);
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
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        // rowData to object array
        Object[] objects = transform(rowData);

        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            redisHandler.insert(objects);
        } else if (kind == RowKind.DELETE) {
            redisHandler.delete(objects);
        }
    }

    public Object[] transform(RowData record) {
        Object[] values = new Object[record.getArity()];
        int idx = 0;
        for (int i = 0; i < record.getArity(); ++i) {
            DataType dataType = dataTypes.get(i);
            values[idx] = this.typeConvertion(dataType.getLogicalType(), record, idx);
            ++idx;
        }
        return values;
    }

    private Object typeConvertion(LogicalType fieldType, RowData rowData, int index) {
        if (rowData.isNullAt(index)) {
            return null;
        } else {
            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    return rowData.getBoolean(index);
                case TINYINT:
                    return rowData.getByte(index);
                case SMALLINT:
                    return rowData.getShort(index);
                case INTEGER:
                    return rowData.getInt(index);
                case BIGINT:
                    return rowData.getLong(index);
                case FLOAT:
                    return rowData.getFloat(index);
                case DOUBLE:
                    return rowData.getDouble(index);
                case CHAR:
                case VARCHAR:
                    return rowData.getString(index).toString();
                case DATE:
                    return rowData.getInt(index); // 实际上是 epoch days
                case TIME_WITHOUT_TIME_ZONE:
                    return rowData.getInt(index); // 实际上是 milliseconds of the day
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return rowData.getTimestamp(index, 3).toLocalDateTime(); // 精度为3，即毫秒级
                case DECIMAL:
                    return rowData.getDecimal(index, ((DecimalType) fieldType).getPrecision(), ((DecimalType) fieldType).getScale()).toBigDecimal();
                case BINARY:
                case VARBINARY:
                    return rowData.getBinary(index);
                // 其他类型处理
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + fieldType);
            }
        }
    }
}
