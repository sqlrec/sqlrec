package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.*;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlTrainModel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelManagerTrainingSafetyTest {
    @Test
    void submissionsAfterCheckpointDeletionAlsoUseDifferentResourceNames() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.db.getCheckpoint("model", "v1")).thenReturn(null);
            ModelManager.trainModel(f.statement, "default");
            String previous = f.train.getId();
            ModelManager.trainModel(f.statement, "default");
            assertNotNull(previous);
            assertNotEquals(previous, f.train.getId());
        }
    }

    @Test
    void repeatedRetriesUseDifferentKubernetesResourceNames() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            ModelManager.trainModel(f.statement, "default");
            String previous = f.train.getId();
            ModelManager.trainModel(f.statement, "default");
            assertNotNull(previous);
            assertNotEquals(previous, f.train.getId());
        }
    }

    @Test
    void failedTrainingRetryPreservesCheckpointAndStopsOldJobBeforeSubmitting() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            Checkpoint old = f.db.getCheckpoint("model", "v1");
            old.setYaml("old-yaml");
            old.setCreatedAt(123L);
            when(f.db.hdfsPathExists("/models/model/v1")).thenReturn(true);
            List<String> events = new ArrayList<>();
            when(f.controller.genModelTrainK8sYaml(f.model, f.train)).thenAnswer(invocation -> {
                events.add("generate");
                return "generated-yaml";
            });
            f.k8s.when(() -> K8sManager.validateYaml("injected-yaml")).thenAnswer(invocation -> {
                events.add("validate");
                return "namespace-bound-yaml";
            });
            f.k8s.when(() -> K8sManager.deleteYamlAndWait("old-yaml")).thenAnswer(invocation -> {
                events.add("stop");
                return null;
            });
            doAnswer(invocation -> {
                events.add("persist");
                return null;
            }).when(f.db).upsertCheckpoint(any());
            f.k8s.when(() -> K8sManager.applyYaml("namespace-bound-yaml")).thenAnswer(invocation -> {
                events.add("apply");
                return null;
            });

            List<CheckpointInfo> result = ModelManager.trainModel(f.statement, "default");

            assertEquals(1, result.size());
            assertEquals("v1", result.get(0).getCheckpointName());
            assertEquals(List.of("generate", "validate", "stop", "persist", "apply"), events);
            verify(f.db).upsertCheckpoint(argThat(checkpoint ->
                    "v1".equals(checkpoint.getCheckpointName())
                            && Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                            && Consts.CHECKPOINT_TYPE_ORIGIN.equals(checkpoint.getCheckpointType())
                            && "namespace-bound-yaml".equals(checkpoint.getYaml())
                            && "model-ddl".equals(checkpoint.getModelDdl())
                            && checkpoint.getCreatedAt() == 123L));
            verify(f.db, never()).insertCheckpoint(any());
            verify(f.db, never()).hdfsPathExists(any());
            f.verifyNoDeletion();
        }
    }

    @Test
    void missingDataAndConfigurationFailuresPreserveExistingCheckpoint() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.controller.requiresTrainingData()).thenReturn(true);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            f.verifyNoDeletion();
            when(f.statement.getDataSource()).thenReturn(new SqlIdentifier("data", SqlParserPos.ZERO));
            when(f.db.getPartitionPaths("default", "data", "")).thenReturn(List.of("hdfs:///data"));
            when(f.controller.genModelTrainK8sYaml(f.model, f.train)).thenThrow(new IllegalArgumentException("bad configuration"));
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
            verify(f.db, never()).upsertCheckpoint(any());
        }
    }

    @Test
    void apiValidationFailureDoesNotSaveCheckpointOrStopPreviousAttempt() throws Exception {
        for (boolean existing : List.of(false, true)) {
            try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
                f.db.getCheckpoint("model", "v1").setYaml("old-yaml");
                if (!existing) when(f.db.getCheckpoint("model", "v1")).thenReturn(null);
                RuntimeException failure = new RuntimeException("Failed to validate YAML: invalid CPU request");
                f.k8s.when(() -> K8sManager.validateYaml("injected-yaml")).thenThrow(failure);

                assertSame(failure, assertThrows(RuntimeException.class,
                        () -> ModelManager.trainModel(f.statement, "default")));
                verify(f.db, never()).insertCheckpoint(any());
                verify(f.db, never()).upsertCheckpoint(any());
                verify(f.db, never()).completeCheckpoint(any(), anyString());
                f.k8s.verify(() -> K8sManager.deleteYamlAndWait(any()), never());
                f.k8s.verify(() -> K8sManager.applyYaml(any()), never());
                f.verifyNoDeletion();
            }
        }
    }

    @Test
    void succeededCheckpointIsReturnedWithoutRetrainingOrDeletingData() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            when(f.controller.requiresTrainingData()).thenReturn(true);
            List<CheckpointInfo> result = ModelManager.trainModel(f.statement, "default");
            assertEquals("v1", result.get(0).getCheckpointName());
            verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
            verify(f.db, never()).upsertCheckpoint(any());
            verify(f.db, never()).insertCheckpoint(any());
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
        }
    }

    @Test
    void runningCheckpointIsReusedAndFailedCreatedJobIsRetriedWithoutDataDeletion() throws Exception {
        for (K8sManager.JobState jobState : K8sManager.JobState.values()) {
            try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_CREATED)) {
                f.db.getCheckpoint("model", "v1").setYaml("old-yaml");
                f.k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml("old-yaml"))
                        .thenReturn(new K8sManager.JobStatus(jobState, null));
                assertEquals("v1", ModelManager.trainModel(f.statement, "default").get(0).getCheckpointName());
                if (jobState == K8sManager.JobState.FAILED) {
                    f.k8s.verify(() -> K8sManager.deleteYamlAndWait("old-yaml"));
                    f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
                    verify(f.db).upsertCheckpoint(argThat(checkpoint ->
                            "injected-yaml".equals(checkpoint.getYaml())
                                    && Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())));
                } else {
                    verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
                    f.k8s.verify(() -> K8sManager.deleteYamlAndWait(any()), never());
                    f.k8s.verify(() -> K8sManager.applyYaml(any()), never());
                }
                f.verifyNoDeletion();
            }
        }
    }

    @Test
    void createdCheckpointWithoutJobMetadataCanBeRetriedWithoutDeletingData() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_CREATED)) {
            ModelManager.trainModel(f.statement, "default");
            verify(f.db).completeCheckpoint(any(), eq(Consts.CHECKPOINT_STATUS_FAILED));
            verify(f.db).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYamlAndWait(any()), never());
            f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
            verify(f.db, never()).insertCheckpoint(any());
            f.verifyNoDeletion();
        }
    }

    @Test
    void unknownJobStatusReportsFailureWithoutReusingOrReplacingCheckpoint() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_CREATED)) {
            f.db.getCheckpoint("model", "v1").setYaml("old-yaml");
            IllegalStateException failure = new IllegalStateException("Cannot determine Job status");
            f.k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml("old-yaml")).thenThrow(failure);

            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> ModelManager.trainModel(f.statement, "default")));
            verify(f.db, never()).completeCheckpoint(any(), anyString());
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.applyYaml(any()), never());
            f.k8s.verify(() -> K8sManager.deleteYamlAndWait(any()), never());
        }
    }

    @Test
    void cleanupFailurePreservesCheckpointAndPreventsSubmission() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            f.db.getCheckpoint("model", "v1").setYaml("old-yaml");
            RuntimeException failure = new RuntimeException("old Pods still terminating");
            f.k8s.when(() -> K8sManager.deleteYamlAndWait("old-yaml")).thenThrow(failure);
            assertSame(failure, assertThrows(RuntimeException.class,
                    () -> ModelManager.trainModel(f.statement, "default")));
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.applyYaml(any()), never());
            f.verifyNoDeletion();
        }
    }

    @Test
    void submissionFailureMarksAttemptFailedAndPreservesItsYamlAndStoredData() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            f.k8s.when(() -> K8sManager.applyYaml("injected-yaml"))
                    .thenThrow(new RuntimeException("submit failed"));
            assertThrows(RuntimeException.class, () -> ModelManager.trainModel(f.statement, "default"));
            verify(f.db).upsertCheckpoint(argThat(checkpoint ->
                    Consts.CHECKPOINT_STATUS_FAILED.equals(checkpoint.getStatus())
                            && "injected-yaml".equals(checkpoint.getYaml())));
            verify(f.db).completeCheckpoint(any(), eq(Consts.CHECKPOINT_STATUS_FAILED));
            f.verifyNoDeletion();
        }
    }

    @Test
    void statusWriteFailureDoesNotHideSubmissionFailure() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            RuntimeException submissionFailure = new RuntimeException("submit failed");
            RuntimeException statusFailure = new RuntimeException("metadata unavailable");
            f.k8s.when(() -> K8sManager.applyYaml("injected-yaml")).thenThrow(submissionFailure);
            when(f.db.completeCheckpoint(any(), eq(Consts.CHECKPOINT_STATUS_FAILED))).thenThrow(statusFailure);

            assertSame(submissionFailure, assertThrows(RuntimeException.class,
                    () -> ModelManager.trainModel(f.statement, "default")));
            assertArrayEquals(new Throwable[]{statusFailure}, submissionFailure.getSuppressed());
            f.verifyNoDeletion();
        }
    }

    @Test
    void failedCheckpointReferencedByServiceCannotBeRetried() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            Service service = new Service();
            service.setName("live-service");
            when(f.db.getServiceListByCheckpoint("model", "v1")).thenReturn(List.of(service));
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.validateYaml("injected-yaml"));
            f.k8s.verifyNoMoreInteractions();
            f.verifyNoDeletion();
        }
    }

    @Test
    void trainingCannotReuseAnExportCheckpoint() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            f.db.getCheckpoint("model", "v1").setCheckpointType(Consts.CHECKPOINT_TYPE_EXPORT);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
            f.k8s.verifyNoInteractions();
            f.verifyNoDeletion();
        }
    }

    @Test
    void newCheckpointStillUsesInsert() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.db.getCheckpoint("model", "v1")).thenReturn(null);
            ModelManager.trainModel(f.statement, "default");
            verify(f.db).insertCheckpoint(argThat(checkpoint ->
                    Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())));
            verify(f.db, never()).upsertCheckpoint(any());
            verify(f.db).hdfsPathExists("/models/model/v1");
            f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
            f.verifyNoDeletion();
        }
    }

    @Test
    void unregisteredTrainingPathIsRejectedBeforeGeneratingOrSubmittingResources() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.db.getCheckpoint("model", "v1")).thenReturn(null);
            when(f.db.hdfsPathExists("/models/model/v1")).thenReturn(true);

            var error = assertThrows(IllegalArgumentException.class,
                    () -> ModelManager.trainModel(f.statement, "default"));

            assertTrue(error.getMessage().contains("/models/model/v1"));
            verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
            verify(f.db, never()).insertCheckpoint(any());
            verify(f.db, never()).upsertCheckpoint(any());
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
        }
    }

    @Test
    void trainingStorageCheckFailurePreventsSubmission() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.db.getCheckpoint("model", "v1")).thenReturn(null);
            RuntimeException failure = new RuntimeException("storage unavailable");
            when(f.db.hdfsPathExists("/models/model/v1")).thenThrow(failure);

            assertSame(failure, assertThrows(RuntimeException.class,
                    () -> ModelManager.trainModel(f.statement, "default")));

            verify(f.db, never()).insertCheckpoint(any());
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
        }
    }
    @Test
    void targetCannotAlsoBeTheIncrementalSource() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.statement.getExistingCheckpoint()).thenReturn(SqlLiteral.createCharString("v1", SqlParserPos.ZERO));
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            f.verifyNoDeletion();
            verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
        }
    }
    @Test
    void unsafeTrainingTargetsFailBeforeGeneratingOrDeletingResources() throws Exception {
        for (String name : List.of(".", "../other/v1", "v1/..")) {
            try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
                f.train.setCheckpointName(name);
                f.train.setModelDir("/models/model/" + name);
                assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
                f.verifyNoDeletion();
                verify(f.controller, never()).genModelTrainK8sYaml(any(), any());
                f.k8s.verifyNoInteractions();
            }
        }
    }

    @Test
    void unsafeSavedCheckpointFailsBeforeStoppingItsJob() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            Checkpoint old = f.db.getCheckpoint("model", "v1");
            old.setCheckpointName(".");
            old.setYaml("running-job");
            when(f.db.getCheckpoint("model", ".")).thenReturn(old);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.deleteCheckpoint("model", "."));
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
        }
    }

    private static final class Fixture extends ModelJobTestFixture {
        final SqlTrainModel statement = mock(SqlTrainModel.class);
        final ModelConf model = new ModelConf();
        final ModelTrainConf train = new ModelTrainConf();

        Fixture(String status) throws Exception {
            Checkpoint old = new Checkpoint(); old.setModelName("model"); old.setCheckpointName("v1"); old.setStatus(status);
            old.setModelDdl(MODEL_DDL);
            old.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
            when(db.getCheckpoint("model", "v1")).thenReturn(old);
            when(db.getServiceListByCheckpoint("model", "v1")).thenReturn(List.of());
            model.setPath("/models/model"); model.setParams(Map.of("model", "gbdt.lightgbm"));
            train.setModelName("model"); train.setCheckpointName("v1"); train.setModelDir("/models/model/v1"); train.setTrainDataPaths(List.of());
            stubModel("model", model);
            converter.when(() -> ModelEntityConverter.convertToModelTrainConf(statement)).thenReturn(train);
            converter.when(() -> ModelEntityConverter.getModelCheckpointPath(old)).thenCallRealMethod();
            when(controller.genModelTrainK8sYaml(model, train)).thenReturn("generated-yaml");
            yaml.when(() -> K8sYamlUtils.injectPodConfig("generated-yaml", train.getParams()))
                    .thenReturn("injected-yaml");
        }
        void verifyNoDeletion() { verify(db, never()).hdfsDeletePath(any()); verify(db, never()).deleteCheckpoint(any(), any()); }
    }
}
