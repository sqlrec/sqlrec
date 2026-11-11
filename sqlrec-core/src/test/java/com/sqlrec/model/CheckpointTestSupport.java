package com.sqlrec.model;

import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.Checkpoint;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;

final class CheckpointTestSupport {
    private CheckpointTestSupport() {}

    static void stubCompletion(MetadataAccess db) {
        lenient().when(db.completeCheckpoint(any(), anyString())).thenAnswer(call -> {
            Checkpoint checkpoint = call.getArgument(0);
            checkpoint.setStatus(call.getArgument(1));
            checkpoint.setUpdatedAt(Math.max(System.currentTimeMillis(), checkpoint.getUpdatedAt() + 1));
            return true;
        });
    }
}
