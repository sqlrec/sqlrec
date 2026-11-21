package com.sqlrec.executor;

import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.model.ModelManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ModelSqlProcessResult extends SqlProcessResult {
    private final CompletableFuture<Void> completion;

    public ModelSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            List<CheckpointInfo> checkpointInfos
    ) {
        super(enumerable, fields);
        completion = ModelManager.watchCheckpointCompletion(
                checkpointInfos == null ? List.of() : List.copyOf(checkpointInfos));
    }

    public static ModelSqlProcessResult msg(String msg, String fieldName, List<CheckpointInfo> checkpointInfos) {
        return new ModelSqlProcessResult(
                RowTransformUtils.getMsgEnumerable(msg),
                DataTypeUtils.getStringTypeField(fieldName),
                checkpointInfos
        );
    }

    @Override
    public boolean isCompleted() {
        if (!completion.isDone()) return false;
        completion.join();
        return true;
    }

    @Override
    public void awaitCompletion(long timeoutMillis) throws InterruptedException, ExecutionException, TimeoutException {
        completion.get(Math.max(0, timeoutMillis), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        completion.cancel(false);
    }
}
