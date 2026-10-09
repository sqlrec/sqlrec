package com.sqlrec.model;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlCreateService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ServiceManagerSafetyTest {
    @Test
    void distinctLogicalServicesCreateAndDeleteDistinctResources() throws Exception {
        try (Fixture f = new Fixture()) {
            ServiceManager.createService(f.statement("rank_a"));
            ServiceManager.createService(f.statement("rank__a"));
            Service a = f.services.get("rank_a");
            Service b = f.services.get("rank__a");
            assertNotEquals(a.getUrl(), b.getUrl());
            var aDeployments = K8sYamlUtils.parseK8sYamlAndGetDeployments(a.getYaml());
            var bDeployments = K8sYamlUtils.parseK8sYamlAndGetDeployments(b.getYaml());
            assertEquals(1, aDeployments.size());
            assertEquals(1, bDeployments.size());
            String aId = aDeployments.getFirst().getMetadata().getName();
            String bId = bDeployments.getFirst().getMetadata().getName();
            assertNotEquals(aId, bId);
            assertEquals(aId, ServiceManager.getServiceConfig("rank_a").getId());
            assertEquals(f.url(aId), a.getUrl());
            assertEquals(f.url(bId), b.getUrl());
            ServiceManager.deleteService("rank_a");
            f.k8s.verify(() -> K8sManager.deleteYaml(a.getYaml()));
            f.k8s.verify(() -> K8sManager.deleteYaml(b.getYaml()), never());
            assertSame(b, f.services.get("rank__a"));
        }
    }

    @Test
    void creatingExistingServiceWithIfNotExistsSkipsConfigurationAndDeployment() throws Exception {
        try (Fixture f = new Fixture()) {
            ServiceManager.createService(f.statement("rank_a"));
            clearInvocations(f.db, f.controller);
            f.k8s.clearInvocations();
            SqlCreateService statement = (SqlCreateService) CompileManager.parseSql(
                    "CREATE SERVICE IF NOT EXISTS RANK_A ON MODEL `rank` CHECKPOINT='missing' WITH ('NAMESPACE'='test')");
            assertEquals("rank_a", ServiceManager.createService(statement));
            verify(f.db).getService("rank_a");
            verify(f.db, never()).getModel(any());
            verify(f.db, never()).getCheckpoint(any(), any());
            verify(f.db, never()).upsertService(any());
            verifyNoInteractions(f.controller);
            f.k8s.verifyNoInteractions();
        }
    }

    @Test
    void deletingServiceWithDifferentCaseInvalidatesConfiguration() throws Exception {
        try (Fixture f = new Fixture()) {
            ServiceManager.createService(f.statement("rank_a"));
            Service service = f.services.get("rank_a");
            assertNotNull(ServiceManager.getServiceConfig("RANK_A"));
            ServiceManager.deleteService("RANK_A");
            f.k8s.verify(() -> K8sManager.deleteYaml(service.getYaml()));
            verify(f.db).deleteService("rank_a");
            assertFalse(f.services.containsKey("rank_a"));
            assertNull(ServiceManager.getServiceConfig("RANK_A"));
        }
    }

    @Test
    void updatingServiceInvalidatesConfigurationReadThroughAnotherCase() throws Exception {
        try (Fixture f = new Fixture()) {
            ServiceManager.createService(f.statement("rank_a"));
            ServiceConf before = ServiceManager.getServiceConfig("RANK_A");
            String changedUrl = "http://changed-endpoint/predict";
            doReturn(changedUrl).when(f.controller).getServiceUrl(any(), any());
            ServiceManager.createService(f.statement("rank_a"));
            ServiceConf after = ServiceManager.getServiceConfig("RANK_A");
            assertNotEquals(before.getUrl(), after.getUrl());
            assertEquals(changedUrl, after.getUrl());
            assertEquals(before.getId(), after.getId());
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MetadataAccess db = mock(MetadataAccess.class);
        final ModelController controller = mock(ModelController.class);
        final Map<String, Service> services = new HashMap<>();
        final Map<ConfigOption<String>, String> defaults = new HashMap<>();
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<ModelControllerFactory> controllers = mockStatic(ModelControllerFactory.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);
        static final String MODEL_DDL = "CREATE MODEL `rank` (x FLOAT) WITH ('MODEL_PATH'='/models/rank')";

        Fixture() throws Exception {
            for (ConfigOption<String> option : List.of(ModelConfigs.JAVA_HOME, ModelConfigs.HADOOP_HOME,
                    ModelConfigs.CLASSPATH, ModelConfigs.HADOOP_CONF_DIR, ModelConfigs.CLIENT_DIR,
                    ModelConfigs.CLIENT_PV_NAME, ModelConfigs.CLIENT_PVC_NAME)) {
                defaults.put(option, option.getDefaultValue());
                option.setDefaultValue("");
            }
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            controllers.when(() -> ModelControllerFactory.getRequiredModelController(any())).thenReturn(controller);
            Model model = new Model();
            model.setDdl(MODEL_DDL);
            when(db.getModel("rank")).thenReturn(model);
            when(db.getService(any())).thenAnswer(i -> services.get(ResourceNames.normalize(i.getArgument(0))));
            doAnswer(i -> {
                Service service = i.getArgument(0);
                services.put(service.getName(), service);
                return null;
            }).when(db).upsertService(any());
            doAnswer(i -> { services.remove(i.getArgument(0)); return null; }).when(db).deleteService(any());
            when(controller.getServiceUrl(any(), any())).thenAnswer(i -> url(((ServiceConf) i.getArgument(1)).getId()));
            when(controller.getServiceK8sYaml(any(), any())).thenAnswer(i -> yaml(((ServiceConf) i.getArgument(1)).getId()));
            ServiceManager.invalidateCache();
        }

        SqlCreateService statement(String name) throws Exception {
            return (SqlCreateService) CompileManager.parseSql(
                    "CREATE SERVICE " + name + " ON MODEL `rank` WITH ('NAMESPACE'='test')");
        }

        String yaml(String id) {
            return """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: %1$s
                      namespace: test
                    spec:
                      selector:
                        matchLabels:
                          app: %1$s
                      template:
                        metadata:
                          labels:
                            app: %1$s
                        spec:
                          containers:
                          - name: serving
                            image: busybox
                    ---
                    apiVersion: v1
                    kind: Service
                    metadata:
                      name: %1$s
                      namespace: test
                    spec:
                      selector:
                        app: %1$s
                    """.formatted(id);
        }

        String url(String id) { return "http://" + id + ".test.svc.cluster.local:80/predict"; }

        public void close() {
            ServiceManager.invalidateCache();
            defaults.forEach(ConfigOption::setDefaultValue);
            k8s.close();
            controllers.close();
            metadata.close();
        }
    }
}
