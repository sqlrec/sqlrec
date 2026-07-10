package com.sqlrec.executor;

import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.model.ModelManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

public class ModelSqlProcessResult extends SqlProcessResult {
    private List<CheckpointInfo> checkpointInfos;
    private volatile long lastCheckTime = 0;
    private volatile boolean cachedCompleted = false;

    public ModelSqlProcessResult() {
        super();
        this.checkpointInfos = new ArrayList<>();
    }

    public ModelSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            List<CheckpointInfo> checkpointInfos
    ) {
        super(enumerable, fields);
        this.checkpointInfos = checkpointInfos != null ? checkpointInfos : new ArrayList<>();
    }

    public static ModelSqlProcessResult msg(String msg, String fieldName, List<CheckpointInfo> checkpointInfos) {
        return new ModelSqlProcessResult(
                DataTransformUtils.getMsgEnumerable(msg),
                DataTypeUtils.getStringTypeField(fieldName),
                checkpointInfos
        );
    }

    public List<CheckpointInfo> getCheckpointInfos() {
        return checkpointInfos;
    }

    public void setCheckpointInfos(List<CheckpointInfo> checkpointInfos) {
        this.checkpointInfos = checkpointInfos;
    }

    @Override
    public boolean isCompleted() {
        if (checkpointInfos.isEmpty()) {
            return true;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastCheckTime < 10000) {
            return cachedCompleted;
        }
        lastCheckTime = currentTime;

        boolean allCompleted = ModelManager.isCheckpointOperationCompleted(checkpointInfos);
        cachedCompleted = allCompleted;
        return allCompleted;
    }
}
