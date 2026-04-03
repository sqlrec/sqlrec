package com.sqlrec.frontend.common;

import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.model.ModelManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.ArrayList;
import java.util.List;

public class ModelSqlProcessResult extends SqlProcessResult {
    private List<CheckpointInfo> checkpointInfos;
    private long lastCheckTime = 0;
    private boolean cachedCompleted = false;

    public ModelSqlProcessResult() {
        super();
        this.checkpointInfos = new ArrayList<>();
    }

    public ModelSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            THandleIdentifier handleIdentifier,
            String queryId,
            String msg,
            List<CheckpointInfo> checkpointInfos
    ) {
        super(enumerable, fields, handleIdentifier, queryId, msg);
        this.checkpointInfos = checkpointInfos != null ? checkpointInfos : new ArrayList<>();
    }

    public List<CheckpointInfo> getCheckpointInfos() {
        return checkpointInfos;
    }

    public void setCheckpointInfos(List<CheckpointInfo> checkpointInfos) {
        this.checkpointInfos = checkpointInfos;
    }

    @Override
    public boolean isCompleted() {
        try {
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
        } catch (Exception e) {
            setException(e);
        }
        return false;
    }
}
