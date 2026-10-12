package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.k8s.K8sManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

class ModelManagerCheckpointFailureTest {
    @Test
    void sharedExportJobUsesOneStatusSnapshotPerPoll() {
        MetadataAccess db = mock(MetadataAccess.class);
        String yaml = "kind: Job\nmetadata:\n  name: export\n";
        Checkpoint user = new Checkpoint();
        user.setModelName("recall_model");
        user.setCheckpointName("v1_export/user");
        user.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        user.setYaml(yaml);
        Checkpoint item = new Checkpoint();
        item.setModelName("recall_model");
        item.setCheckpointName("v1_export/item");
        item.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        item.setYaml(yaml);
        when(db.getCheckpoint("recall_model", "v1_export/user")).thenReturn(user);
        when(db.getCheckpoint("recall_model", "v1_export/item")).thenReturn(item);
        List<CheckpointInfo> checkpoints = List.of(
                new CheckpointInfo("recall_model", "v1_export/user"),
                new CheckpointInfo("recall_model", "v1_export/item"));

        try (MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
             MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class)) {
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml(yaml))
                    .thenReturn(new K8sManager.JobStatus("running", null),
                            new K8sManager.JobStatus("succeeded", null));

            assertFalse(ModelManager.isCheckpointOperationCompleted(checkpoints));
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, user.getStatus());
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, item.getStatus());
            k8s.verify(() -> K8sManager.checkJobsStatusDetailFromYaml(yaml), times(1));
            k8s.verify(() -> K8sManager.deleteYaml(yaml), never());

            assertTrue(ModelManager.isCheckpointOperationCompleted(checkpoints));
            assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, user.getStatus());
            assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, item.getStatus());
            k8s.verify(() -> K8sManager.checkJobsStatusDetailFromYaml(yaml), times(2));
            k8s.verify(() -> K8sManager.deleteYaml(yaml), times(1));
            verify(db).upsertCheckpoint(user);
            verify(db).upsertCheckpoint(item);
        }
    }

    @Test
    void imagePullFailureMarksCheckpointFailedAndReachesSqlCaller() {
        MetadataAccess db = mock(MetadataAccess.class);
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName("rank_model");
        checkpoint.setCheckpointName("v1");
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setYaml("kind: Job\nmetadata:\n  name: train\n");
        when(db.getCheckpoint("rank_model", "v1")).thenReturn(checkpoint);

        try (MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
             MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class)) {
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml(checkpoint.getYaml()))
                    .thenReturn(new K8sManager.JobStatus("failed",
                            "Pod train-abc: container trainer is waiting: ErrImagePull"));

            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> ModelManager.isCheckpointOperationCompleted(
                            List.of(new CheckpointInfo("rank_model", "v1"))));

            assertTrue(error.getMessage().contains("ErrImagePull"));
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, checkpoint.getStatus());
            verify(db).upsertCheckpoint(checkpoint);
        }
    }

    @Test
    void failedCheckpointIsKeptIfOldJobCannotBeDeletedForRetry() {
        MetadataAccess db = mock(MetadataAccess.class);
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName("rank_model");
        checkpoint.setCheckpointName("v1");
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_FAILED);
        checkpoint.setModelDdl("CREATE MODEL rank_model (x FLOAT) WITH ('MODEL_PATH'='/models/rank_model')");
        checkpoint.setYaml("kind: Job\nmetadata:\n  name: train\n");
        when(db.getCheckpoint("rank_model", "v1")).thenReturn(checkpoint);
        when(db.getServiceListByCheckpoint("rank_model", "v1")).thenReturn(List.of());

        try (MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
             MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class)) {
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            k8s.when(() -> K8sManager.deleteYamlAndWait(checkpoint.getYaml()))
                    .thenThrow(new RuntimeException("old Job still terminating"));

            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> ModelManager.deleteCheckpoint("rank_model", "v1"));

            assertTrue(error.getMessage().contains("old Job still terminating"));
            verify(db, never()).deleteCheckpoint("rank_model", "v1");
            verify(db, never()).hdfsDeletePath(org.mockito.ArgumentMatchers.anyString());
        }
    }

    @Test
    void missingJobMakesCreatedCheckpointEligibleForImmediateRetry() {
        MetadataAccess db = mock(MetadataAccess.class);
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName("rank_model");
        checkpoint.setCheckpointName("v1");
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setYaml("kind: Job\nmetadata:\n  name: train\n");

        try (MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class)) {
            k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml(checkpoint.getYaml()))
                    .thenReturn(new K8sManager.JobStatus("failed", "Job not found"));

            assertFalse(ModelManager.canReuseCreatedCheckpoint(db, checkpoint));
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, checkpoint.getStatus());
            verify(db).upsertCheckpoint(checkpoint);
        }
    }

    @Test
    void runningJobReusesCreatedCheckpoint() {
        MetadataAccess db = mock(MetadataAccess.class);
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setYaml("kind: Job\nmetadata:\n  name: train\n");

        try (MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class)) {
            k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml(checkpoint.getYaml()))
                    .thenReturn(new K8sManager.JobStatus("running", null));

            assertTrue(ModelManager.canReuseCreatedCheckpoint(db, checkpoint));
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, checkpoint.getStatus());
            verify(db, never()).upsertCheckpoint(checkpoint);
        }
    }
}
