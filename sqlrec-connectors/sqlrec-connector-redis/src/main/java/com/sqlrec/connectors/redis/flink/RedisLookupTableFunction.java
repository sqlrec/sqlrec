package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RedisLookupTableFunction extends AsyncTableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private RedisConfig redisConfig;
    private final List<DataType> columnDataTypes;
    private transient RedisHandler redisHandler;

    public RedisLookupTableFunction(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        this.columnDataTypes = tableSchema.getColumnDataTypes();
    }

    /** Test-only: inject a handler without opening a Redis connection. */
    void setRedisHandlerForTest(RedisHandler handler) {
        this.redisHandler = handler;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
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

    public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object rowkey) {
        if (rowkey == null) {
            resultFuture.complete(java.util.Collections.emptyList());
            return;
        }
        try {
            redisHandler.scan(rowkey.toString())
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            resultFuture.completeExceptionally(throwable);
                            return;
                        }
                        try {
                            List<GenericRowData> rows = new ArrayList<>();
                            for (Object[] objects : result) {
                                rows.add(FlinkSchemaUtils.toRowData(objects, columnDataTypes));
                            }
                            resultFuture.complete(rows);
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                        }
                    });
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }
}
