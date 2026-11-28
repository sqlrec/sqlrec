package com.sqlrec.model;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Model;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class ModelJobTestFixture implements AutoCloseable {
    static final String MODEL_DDL = "model-ddl";

    final MetadataAccess db = mock(MetadataAccess.class);
    final ModelController controller = mock(ModelController.class);
    private final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
    final MockedStatic<ModelEntityConverter> converter = mockStatic(ModelEntityConverter.class);
    private final MockedStatic<ModelControllerFactory> controllers = mockStatic(ModelControllerFactory.class);
    final MockedStatic<K8sYamlUtils> yaml = mockStatic(K8sYamlUtils.class);
    final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);

    ModelJobTestFixture() {
        CheckpointTestSupport.stubCompletion(db);
        metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
        yaml.when(() -> K8sYamlUtils.checkpointAttemptJobId(anyString(), anyString(), anyString())).thenCallRealMethod();
    }

    final void stubModel(String name, ModelConf model) throws Exception {
        Model entity = new Model();
        entity.setDdl(MODEL_DDL);
        when(db.getModel(name)).thenReturn(entity);
        converter.when(() -> ModelEntityConverter.convertToModel(MODEL_DDL)).thenReturn(model);
        controllers.when(() -> ModelControllerFactory.getRequiredModelController(model)).thenReturn(controller);
    }

    @Override
    public final void close() {
        k8s.close();
        yaml.close();
        controllers.close();
        converter.close();
        metadata.close();
    }
}
