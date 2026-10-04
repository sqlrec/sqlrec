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
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelManagerTrainingSafetyTest {
    @Test
    void existingCheckpointIsReplacedAfterValidation() throws Exception {
        for (String status : List.of(Consts.CHECKPOINT_STATUS_FAILED, Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            try (Fixture f = new Fixture(status)) {
                List<CheckpointInfo> result = ModelManager.trainModel(f.statement, "default");
                assertEquals(1, result.size());
                assertEquals("v1", result.get(0).getCheckpointName());
                org.mockito.InOrder order = inOrder(f.controller, f.db);
                order.verify(f.controller).genModelTrainK8sYaml(f.model, f.train);
                order.verify(f.db).hdfsDeletePath("/models/model/v1");
                order.verify(f.db).deleteCheckpoint("model", "v1");
                order.verify(f.db).insertCheckpoint(argThat(checkpoint ->
                        "v1".equals(checkpoint.getCheckpointName())
                                && Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                                && "injected-yaml".equals(checkpoint.getYaml())));
                f.k8s.verify(() -> K8sManager.applyYaml("injected-yaml"));
            }
        }
    }
    @Test
    void missingDataAndConfigurationFailuresPreserveExistingCheckpoint() throws Exception {
        for (String status : List.of(Consts.CHECKPOINT_STATUS_FAILED, Consts.CHECKPOINT_STATUS_SUCCEEDED)) {
            try (Fixture f = new Fixture(status)) {
                when(f.controller.requiresTrainingData()).thenReturn(true);
                assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
                f.verifyNoDeletion();
                f.train.setTrainDataPaths(List.of("/data"));
                when(f.controller.genModelTrainK8sYaml(f.model, f.train)).thenThrow(new IllegalArgumentException("bad configuration"));
                assertThrows(IllegalArgumentException.class, () -> ModelManager.trainModel(f.statement, "default"));
                f.verifyNoDeletion();
            }
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
            when(db.getCheckpoint("model", "v1")).thenReturn(old);
            when(db.getServiceListByCheckpoint("model", "v1")).thenReturn(List.of());
            model.setPath("/models/model"); model.setParams(Map.of("model", "gbdt.lightgbm"));
            train.setModelName("model"); train.setCheckpointName("v1"); train.setModelDir("/models/model/v1"); train.setTrainDataPaths(List.of());
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            converter.when(() -> ModelEntityConverter.convertToModelTrainConf(statement, "default")).thenReturn(train);
            converter.when(() -> ModelEntityConverter.convertToModel("model-ddl")).thenReturn(model);
            converter.when(() -> ModelEntityConverter.getModelCheckpointPath(old)).thenReturn("/models/model/v1");
            controllers.when(() -> ModelControllerFactory.getRequiredModelController(model)).thenReturn(controller);
            when(controller.genModelTrainK8sYaml(model, train)).thenReturn("generated-yaml");
            yaml.when(() -> K8sYamlUtils.injectPodConfig("generated-yaml", model, train.getParams()))
                    .thenReturn("injected-yaml");
        }
        void verifyNoDeletion() { verify(db, never()).hdfsDeletePath(any()); verify(db, never()).deleteCheckpoint(any(), any()); }
        public void close() { k8s.close(); yaml.close(); controllers.close(); converter.close(); metadata.close(); }
    }
}
