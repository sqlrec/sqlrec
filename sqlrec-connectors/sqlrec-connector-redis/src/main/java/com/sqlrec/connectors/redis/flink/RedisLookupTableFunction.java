package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RedisLookupTableFunction extends AsyncTableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private RedisConfig redisConfig;
    private ResolvedSchema tableSchema;
    private transient RedisHandler redisHandler;

    public RedisLookupTableFunction(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        this.tableSchema = tableSchema;
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
        redisHandler.scan(rowkey.toString())
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                        return;
                    }
                    List<GenericRowData> rows = new ArrayList<>();
                    for (Object[] objects : result) {
                        GenericRowData rowData = new GenericRowData(redisConfig.fieldSchemas.size());
                        for (int i = 0; i < redisConfig.fieldSchemas.size(); i++) {
                            rowData.setField(i, objects[i]);
                        }
                        rows.add(rowData);
                    }
                    resultFuture.complete(rows);
                });
    }
}
