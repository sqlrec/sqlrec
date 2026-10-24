package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.*;
import com.sqlrec.db.*;
import com.sqlrec.entity.*;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlTrainModel;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelManagerTrainingSafetyTest {
    @Test
    void failedTrainingRetryPreservesCheckpointAndStopsOldJobBeforeSubmitting() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            Checkpoint old = f.db.getCheckpoint("model", "v1");
            old.setYaml("old-yaml");
            old.setCreatedAt(123L);
            List<String> events = new ArrayList<>();
            when(f.controller.genModelTrainK8sYaml(f.model, f.train)).thenAnswer(invocation -> {
                events.add("generate");
                return "generated-yaml";
            });
            f.k8s.when(() -> K8sManager.deleteYamlAndWait("old-yaml")).thenAnswer(invocation -> {
                events.add("stop");
                return null;
            });
            doAnswer(invocation -> {
                events.add("persist");
                return null;
            }).when(f.db).upsertCheckpoint(any());
            f.k8s.when(() -> K8sManager.applyYaml("injected-yaml")).thenAnswer(invocation -> {
                events.add("apply");
                return null;
            });

            List<CheckpointInfo> result = ModelManager.trainModel(f.statement, "default");

            assertEquals(1, result.size());
            assertEquals("v1", result.get(0).getCheckpointName());
            assertEquals(List.of("generate", "stop", "persist", "apply"), events);
            verify(f.db).upsertCheckpoint(argThat(checkpoint ->
                    "v1".equals(checkpoint.getCheckpointName())
                            && Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                            && Consts.CHECKPOINT_TYPE_ORIGIN.equals(checkpoint.getCheckpointType())
                            && "injected-yaml".equals(checkpoint.getYaml())
                            && "model-ddl".equals(checkpoint.getModelDdl())
                            && checkpoint.getCreatedAt() == 123L));
            verify(f.db, never()).insertCheckpoint(any());
            f.verifyNoDeletion();
        }
    }

    @Test
    void missingDataAndConfigurationFailuresPreserveExistingCheckpoint() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            when(f.controller.requiresTrainingData()).thenReturn(true);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            f.verifyNoDeletion();
            f.train.setTrainDataPaths(List.of("/data"));
            when(f.controller.genModelTrainK8sYaml(f.model, f.train)).thenThrow(new IllegalArgumentException("bad configuration"));
            assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
            f.verifyNoDeletion();
            f.k8s.verifyNoInteractions();
            verify(f.db, never()).upsertCheckpoint(any());
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
        for (String jobState : List.of("running", "succeeded", "failed")) {
            try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_CREATED)) {
                f.db.getCheckpoint("model", "v1").setYaml("old-yaml");
                f.k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml("old-yaml"))
                        .thenReturn(new K8sManager.JobStatus(jobState, null));
                assertEquals("v1", ModelManager.trainModel(f.statement, "default").get(0).getCheckpointName());
                if (jobState.equals("failed")) {
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
            verify(f.db, times(2)).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYamlAndWait(any()), never());
            f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
            verify(f.db, never()).insertCheckpoint(any());
            f.verifyNoDeletion();
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
    void submissionFailureLeavesUpdatedRetryMetadataAndStoredData() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            f.k8s.when(() -> K8sManager.applyYaml("injected-yaml"))
                    .thenThrow(new RuntimeException("submit failed"));
            assertThrows(RuntimeException.class, () -> ModelManager.trainModel(f.statement, "default"));
            verify(f.db).upsertCheckpoint(argThat(checkpoint ->
                    Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                            && "injected-yaml".equals(checkpoint.getYaml())));
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
            f.k8s.verifyNoInteractions();
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
            f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
            f.verifyNoDeletion();
        }
    }
    @Test
    void targetCannotAlsoBeTheIncrementalSource() throws Exception {
        try (Fixture f = new Fixture(Consts.CHECKPOINT_STATUS_FAILED)) {
            f.train.setBaseModelDir("/models/model/v1");
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

    private static class Fixture implements AutoCloseable {
        final MetadataAccess db = mock(MetadataAccess.class);
        final SqlTrainModel statement = mock(SqlTrainModel.class);
        final ModelController controller = mock(ModelController.class);
        final ModelConf model = new ModelConf();
        final ModelTrainConf train = new ModelTrainConf();
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<ModelEntityConverter> converter = mockStatic(ModelEntityConverter.class);
        final MockedStatic<ModelControllerFactory> controllers = mockStatic(ModelControllerFactory.class);
        final MockedStatic<K8sYamlUtils> yaml = mockStatic(K8sYamlUtils.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);
        Fixture(String status) throws Exception {
            Model entity = new Model(); entity.setDdl("model-ddl"); when(db.getModel("model")).thenReturn(entity);
            Checkpoint old = new Checkpoint(); old.setModelName("model"); old.setCheckpointName("v1"); old.setStatus(status);
            old.setModelDdl("model-ddl");
            old.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
            when(db.getCheckpoint("model", "v1")).thenReturn(old);
            when(db.getServiceListByCheckpoint("model", "v1")).thenReturn(List.of());
            model.setPath("/models/model"); model.setParams(Map.of("model", "gbdt.lightgbm"));
            train.setModelName("model"); train.setCheckpointName("v1"); train.setModelDir("/models/model/v1"); train.setTrainDataPaths(List.of());
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            converter.when(() -> ModelEntityConverter.convertToModelTrainConf(statement, "default")).thenReturn(train);
            converter.when(() -> ModelEntityConverter.convertToModel("model-ddl")).thenReturn(model);
            converter.when(() -> ModelEntityConverter.getModelCheckpointPath(old)).thenCallRealMethod();
            controllers.when(() -> ModelControllerFactory.getRequiredModelController(model)).thenReturn(controller);
            when(controller.genModelTrainK8sYaml(model, train)).thenReturn("generated-yaml");
            yaml.when(() -> K8sYamlUtils.injectPodConfig("generated-yaml", model, train.getParams()))
                    .thenReturn("injected-yaml");
        }
        void verifyNoDeletion() { verify(db, never()).hdfsDeletePath(any()); verify(db, never()).deleteCheckpoint(any(), any()); }
        public void close() { k8s.close(); yaml.close(); controllers.close(); converter.close(); metadata.close(); }
    }
}
