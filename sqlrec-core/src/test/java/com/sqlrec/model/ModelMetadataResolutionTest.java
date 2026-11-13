package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelMetadataResolutionTest {
    @Test
    void trainingResolvesSnapshotPathsAndPartitionsWithOneLookupPerEntity() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1", "/models/rank/history", Consts.CHECKPOINT_STATUS_FAILED);
            f.checkpoint("base", "/archive/rank", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            when(f.db.getPartitionPaths("features", "samples", ""))
                    .thenReturn(List.of("hdfs:///warehouse/samples"));
            var statement = (SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL `rank` CHECKPOINT='v1' ON samples FROM 'base'");

            ModelManager.trainModel(statement, "features");

            ArgumentCaptor<ModelTrainConf> config = ArgumentCaptor.forClass(ModelTrainConf.class);
            verify(f.controller).genModelTrainK8sYaml(any(), config.capture());
            assertEquals("/models/rank/history/v1", config.getValue().getModelDir());
            assertEquals("/archive/rank/base", config.getValue().getBaseModelDir());
            assertEquals(List.of("hdfs:///warehouse/samples/*"), config.getValue().getTrainDataPaths());
            verify(f.db).getModel("rank");
            verify(f.db).getCheckpoint("rank", "v1");
            verify(f.db).getCheckpoint("rank", "base");
            verify(f.db).getPartitionPaths("features", "samples", "");
        }
    }

    @Test
    void newTrainingCheckpointUsesTheCurrentModelPath() throws Exception {
        try (Fixture f = new Fixture()) {
            ModelManager.trainModel((SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL `rank` CHECKPOINT='new/nested'"), "default");

            ArgumentCaptor<ModelTrainConf> config = ArgumentCaptor.forClass(ModelTrainConf.class);
            verify(f.controller).genModelTrainK8sYaml(any(), config.capture());
            assertEquals("/models/rank/new/nested", config.getValue().getModelDir());
            assertEquals(List.of(), config.getValue().getTrainDataPaths());
            verify(f.db).getModel("rank");
            verify(f.db).getCheckpoint("rank", "new/nested");
            verify(f.db, never()).getPartitionPaths(any(), any(), any());
        }
    }

    @Test
    void exportUsesTheSourceSnapshotWithoutReadingItTwice() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1", "/archive/rank", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            ModelManager.exportModel((SqlExportModel) CompileManager.parseSql(
                    "EXPORT MODEL `rank` CHECKPOINT='v1'"), "default");

            ArgumentCaptor<ModelExportConf> config = ArgumentCaptor.forClass(ModelExportConf.class);
            verify(f.controller).genModelExportK8sYaml(any(), config.capture());
            assertEquals("/archive/rank/v1", config.getValue().getBaseModelDir());
            verify(f.db).getModel("rank");
            verify(f.db).getCheckpoint("rank", "v1");
        }
    }

    @Test
    void serviceCreationAndCacheLoadingRetainTheirCheckpointSnapshot() throws Exception {
        try (Fixture f = new Fixture()) {
            Checkpoint checkpoint = f.checkpoint("v1", "/archive/rank", Consts.CHECKPOINT_STATUS_SUCCEEDED);
            AtomicReference<Service> saved = new AtomicReference<>();
            doAnswer(call -> { saved.set(call.getArgument(0)); return null; }).when(f.db).upsertService(any());
            when(f.db.getService("rank-service")).thenAnswer(call -> saved.get());

            ServiceManager.createService(f.serviceStatement());

            ArgumentCaptor<ServiceConf> config = ArgumentCaptor.forClass(ServiceConf.class);
            verify(f.controller).getServiceUrl(any(), config.capture());
            assertEquals("/archive/rank/v1", config.getValue().getModelCheckpointDir());
            assertEquals("/models/rank", config.getValue().getModelConfig().getPath());
            assertEquals(checkpoint.getModelDdl(), saved.get().getModelDdl());
            verify(f.db).getModel("rank");
            verify(f.db).getCheckpoint("rank", "v1");

            clearInvocations(f.db);
            ServiceConf cached = ServiceManager.getServiceConfig("rank-service");
            assertEquals("/archive/rank/v1", cached.getModelCheckpointDir());
            assertEquals("/archive/rank", cached.getModelConfig().getPath());
            verify(f.db).getCheckpoint("rank", "v1");
            verify(f.db, never()).getModel(any());

            when(f.db.getCheckpoint("rank", "v1")).thenReturn(null);
            ServiceManager.invalidateCache();
            ServiceConf fallback = ServiceManager.getServiceConfig("rank-service");
            assertEquals("/models/rank/v1", fallback.getModelCheckpointDir());
            assertEquals("/archive/rank", fallback.getModelConfig().getPath());
        }
    }

    @Test
    void fileServiceLoadingUsesTheSuppliedMetadata() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1", "/archive/rank", Consts.CHECKPOINT_STATUS_FAILED);
            f.metadata.when(MetadataAccessFactory::getInstance).thenThrow(new AssertionError("global metadata lookup"));

            Service service = ServiceManager.loadServiceDefinition(f.serviceStatement(), f.db);

            assertEquals("rank-service", service.getName());
            assertEquals("", service.getYaml());
            assertEquals("http://rank-service/predict", service.getUrl());
            assertEquals(f.modelDdl("/models/rank"), service.getModelDdl());
            f.metadata.verifyNoInteractions();
            verify(f.db).getModel("rank");
            verify(f.db).getCheckpoint("rank", "v1");
            verify(f.db).upsertService(service);
            verify(f.controller, never()).validateServiceCheckpointType(any());
            verify(f.controller, never()).getServiceK8sYaml(any(), any());
            f.k8s.verifyNoInteractions();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MetadataAccess db = mock(MetadataAccess.class);
        final ModelController controller = mock(ModelController.class);
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<ModelControllerFactory> controllers = mockStatic(ModelControllerFactory.class);
        final MockedStatic<K8sYamlUtils> yaml = mockStatic(K8sYamlUtils.class, CALLS_REAL_METHODS);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);

        Fixture() throws Exception {
            Model model = new Model();
            model.setDdl(modelDdl("/models/rank"));
            when(db.getModel("rank")).thenReturn(model);
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            controllers.when(() -> ModelControllerFactory.getRequiredModelController(any())).thenReturn(controller);
            when(controller.genModelTrainK8sYaml(any(), any())).thenReturn("");
            when(controller.genModelExportK8sYaml(any(), any())).thenReturn("");
            when(controller.getExportCheckpoints(any())).thenReturn(List.of("export"));
            when(controller.getServiceUrl(any(), any())).thenReturn("http://rank-service/predict");
            when(controller.getServiceK8sYaml(any(), any())).thenReturn("");
            yaml.when(() -> K8sYamlUtils.injectPodConfig(anyString(), any(), any())).thenAnswer(call -> call.getArgument(0));
            ServiceManager.invalidateCache();
        }

        Checkpoint checkpoint(String name, String path, String status) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName("rank");
            checkpoint.setCheckpointName(name);
            checkpoint.setModelDdl(modelDdl(path));
            checkpoint.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
            checkpoint.setStatus(status);
            when(db.getCheckpoint("rank", name)).thenReturn(checkpoint);
            return checkpoint;
        }

        SqlCreateService serviceStatement() throws Exception {
            return (SqlCreateService) CompileManager.parseSql(
                    "CREATE SERVICE `rank-service` ON MODEL `rank` CHECKPOINT='v1'");
        }

        private String modelDdl(String path) {
            return "CREATE MODEL `rank` (x FLOAT) WITH ('MODEL_PATH'='" + path + "')";
        }

        @Override
        public void close() {
            ServiceManager.invalidateCache();
            k8s.close();
            yaml.close();
            controllers.close();
            metadata.close();
        }
    }
}
