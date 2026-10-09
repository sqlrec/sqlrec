package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlExportModel;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ModelManagerExportTest {
    @Test
    void reusesRunningExportButStillChecksEveryRequestedCheckpoint() throws Exception {
        try (ExportFixture fixture = new ExportFixture()) {
            Checkpoint running = fixture.checkpoint("export_a", Consts.CHECKPOINT_STATUS_CREATED);
            running.setYaml("running-job");
            Checkpoint missingYaml = fixture.checkpoint("export_b", Consts.CHECKPOINT_STATUS_CREATED);
            fixture.k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml("running-job"))
                    .thenReturn(new K8sManager.JobStatus("running", null));

            List<CheckpointInfo> result = fixture.export();

            assertEquals(1, result.size());
            assertEquals("export_a", result.get(0).getCheckpointName());
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, missingYaml.getStatus());
            verify(fixture.db).upsertCheckpoint(missingYaml);
            assertTrue(fixture.events.isEmpty(), "Reusing an export must not delete, create or submit resources");
            verify(fixture.controller, never()).getExportCleanPath(any());
            verify(fixture.db, never()).deleteCheckpoint(any(), any());
            fixture.k8s.verify(() -> K8sManager.applyYaml(any()), never());
        }
    }

    @Test
    void validatesBeforeReplacingExportsAndPersistsBeforeApplyingYaml() throws Exception {
        for (String status : List.of(Consts.CHECKPOINT_STATUS_FAILED, Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            try (ExportFixture fixture = new ExportFixture()) {
                fixture.checkpoint("export_a", status);

                List<CheckpointInfo> result = fixture.export();

                assertEquals(List.of("export_a", "export_b"),
                        result.stream().map(CheckpointInfo::getCheckpointName).toList());
                assertEquals(List.of(
                        "generate", "remove:/models/rank/export_a", "delete:export_a",
                        "remove:/models/rank/export",
                        "insert:export_a", "insert:export_b", "apply"), fixture.events);
                assertEquals(2, fixture.inserted.size());
                for (Checkpoint checkpoint : fixture.inserted) {
                    assertEquals("rank", checkpoint.getModelName());
                    assertEquals("injected-yaml", checkpoint.getYaml());
                    assertEquals(Consts.CHECKPOINT_TYPE_EXPORT, checkpoint.getCheckpointType());
                    assertEquals(Consts.CHECKPOINT_STATUS_CREATED, checkpoint.getStatus());
                }
            }
        }
    }

    @Test
    void cleanupFailureStopsGenerationPersistenceAndSubmission() throws Exception {
        try (ExportFixture fixture = new ExportFixture()) {
            RuntimeException failure = new RuntimeException("cleanup failed");
            doThrow(failure).when(fixture.db).hdfsDeletePath("/models/rank/export");

            assertSame(failure, assertThrows(RuntimeException.class, fixture::export));
            verify(fixture.controller).genModelExportK8sYaml(any(), any());
            verify(fixture.db, never()).insertCheckpoint(any());
            fixture.k8s.verify(() -> K8sManager.applyYaml(any()), never());
        }
    }

    @Test
    void laterReferencedExportPreventsAnyEarlierDeletion() throws Exception {
        try (ExportFixture fixture = new ExportFixture()) {
            fixture.checkpoint("export_a", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            fixture.checkpoint("export_b", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            com.sqlrec.entity.Service service = new com.sqlrec.entity.Service();
            service.setName("live-serving");
            when(fixture.db.getServiceListByCheckpoint("rank", "export_b")).thenReturn(List.of(service));
            assertThrows(IllegalArgumentException.class, fixture::export);
            verify(fixture.db, never()).hdfsDeletePath(any());
            verify(fixture.db, never()).deleteCheckpoint(any(), any());
            verify(fixture.db, never()).insertCheckpoint(any());
            fixture.k8s.verifyNoInteractions();
        }
    }

    @Test
    void laterUnsafeExportCheckpointPreventsAnyEarlierDeletion() throws Exception {
        try (ExportFixture fixture = new ExportFixture()) {
            for (String name : List.of("export_a", "export_b")) {
                Checkpoint old = fixture.checkpoint(name, Consts.CHECKPOINT_STATUS_FAILED);
                old.setYaml("old-job");
                if (name.equals("export_b")) old.setCheckpointName(".");
            }
            assertThrows(IllegalArgumentException.class, fixture::export);
            verify(fixture.db, never()).hdfsDeletePath(any());
            verify(fixture.db, never()).deleteCheckpoint(any(), any());
            fixture.k8s.verifyNoInteractions();
        }
    }

    private static final class ExportFixture implements AutoCloseable {
        private final MetadataAccess db = mock(MetadataAccess.class);
        private final SqlExportModel statement = mock(SqlExportModel.class);
        private final ModelController controller = mock(ModelController.class);
        private final List<String> events = new ArrayList<>();
        private final List<Checkpoint> inserted = new ArrayList<>();
        private final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        private final MockedStatic<ModelEntityConverter> converter = mockStatic(ModelEntityConverter.class);
        private final MockedStatic<ModelControllerFactory> controllers = mockStatic(ModelControllerFactory.class);
        private final MockedStatic<K8sYamlUtils> yaml = mockStatic(K8sYamlUtils.class);
        private final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);

        private ExportFixture() throws Exception {
            Model modelEntity = new Model();
            modelEntity.setDdl("model-ddl");
            ModelConf model = new ModelConf();
            model.setPath("/models/rank");
            ModelExportConf export = new ModelExportConf();
            export.setModelName("rank");
            export.setCheckpointName("source");
            export.setParams(Map.of());

            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            converter.when(() -> ModelEntityConverter.convertToModelExportConf(statement, "default"))
                    .thenReturn(export);
            converter.when(() -> ModelEntityConverter.convertToModel("model-ddl")).thenReturn(model);
            controllers.when(() -> ModelControllerFactory.getRequiredModelController(model)).thenReturn(controller);
            when(db.getModel("rank")).thenReturn(modelEntity);
            checkpoint("source", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            when(controller.getExportCheckpoints(export)).thenReturn(List.of("export_a", "export_b"));
            when(controller.getExportCleanPath(export)).thenReturn("/models/rank/export");
            when(controller.genModelExportK8sYaml(model, export)).thenAnswer(invocation -> {
                events.add("generate");
                return "generated-yaml";
            });
            yaml.when(() -> K8sYamlUtils.injectPodConfig("generated-yaml", model, export.getParams()))
                    .thenReturn("injected-yaml");
            doAnswer(invocation -> {
                events.add("remove:" + invocation.getArgument(0));
                return null;
            }).when(db).hdfsDeletePath(any());
            doAnswer(invocation -> {
                events.add("delete:" + invocation.getArgument(1));
                return null;
            }).when(db).deleteCheckpoint(any(), any());
            doAnswer(invocation -> {
                Checkpoint checkpoint = invocation.getArgument(0);
                inserted.add(checkpoint);
                events.add("insert:" + checkpoint.getCheckpointName());
                return null;
            }).when(db).insertCheckpoint(any());
            k8s.when(() -> K8sManager.applyYaml("injected-yaml")).thenAnswer(invocation -> {
                events.add("apply");
                return null;
            });
        }

        private Checkpoint checkpoint(String name, String status) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName("rank");
            checkpoint.setCheckpointName(name);
            checkpoint.setStatus(status);
            checkpoint.setModelDdl("model-ddl");
            converter.when(() -> ModelEntityConverter.getModelCheckpointPath(checkpoint)).thenCallRealMethod();
            when(db.getCheckpoint("rank", name)).thenReturn(checkpoint);
            return checkpoint;
        }

        private List<CheckpointInfo> export() throws Exception {
            return ModelManager.exportModel(statement, "default");
        }

        @Override
        public void close() {
            k8s.close();
            yaml.close();
            controllers.close();
            converter.close();
            metadata.close();
        }
    }
    @Test
    void failedOrRunningSourceIsRejectedBeforeCleanupOrSubmission() throws Exception {
        for (String status : List.of(Consts.CHECKPOINT_STATUS_FAILED, Consts.CHECKPOINT_STATUS_CREATED)) {
            try (ExportFixture fixture = new ExportFixture()) {
                fixture.checkpoint("source", status);
                assertThrows(IllegalArgumentException.class, fixture::export);
                assertTrue(fixture.events.isEmpty());
                verify(fixture.db, never()).hdfsDeletePath(any());
            }
        }
    }

    @Test
    void configurationGenerationFailurePreservesExistingExports() throws Exception {
        for (String status : List.of(Consts.CHECKPOINT_STATUS_FAILED, Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            try (ExportFixture fixture = new ExportFixture()) {
                fixture.checkpoint("export_a", status);
                when(fixture.controller.genModelExportK8sYaml(any(), any())).thenThrow(new IllegalArgumentException("bad config"));
                assertThrows(IllegalArgumentException.class, fixture::export);
                verify(fixture.db, never()).deleteCheckpoint(any(), any());
                verify(fixture.db, never()).hdfsDeletePath(any());
                fixture.k8s.verify(() -> K8sManager.applyYaml(any()), never());
            }
        }
    }

}
