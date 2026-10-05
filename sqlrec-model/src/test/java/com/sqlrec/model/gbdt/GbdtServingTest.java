package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GbdtServingTest {
    @Test
    void allBackendsInheritModelSettingsAndWaitForHealthyServer() {
        for (ModelController controller : List.of(new LightGBMModel(), new XGBoostModel(), new CatBoostModel())) {
            ModelConf model = new ModelConf();
            Map<String, String> params = Map.of("image", "custom/gbdt", "version", "v2",
                    "pod_memory", "8Gi", "pod_cpu_cores", "4", "replicas", "2");
            model.setParams(params);
            ServiceConf service = service(Map.of());
            Deployment deployment = deployment(controller.getServiceK8sYaml(model, service));
            Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
            assertEquals("custom/gbdt:v2", container.getImage());
            assertEquals("8", container.getResources().getRequests().get("memory").getAmount());
            assertEquals("Gi", container.getResources().getRequests().get("memory").getFormat());
            assertEquals("4", container.getResources().getRequests().get("cpu").getAmount());
            assertEquals(2, deployment.getSpec().getReplicas());
            assertEquals("/health", container.getReadinessProbe().getHttpGet().getPath());
            assertEquals(80, container.getReadinessProbe().getHttpGet().getPort().getIntVal());
            assertEquals("/health", container.getStartupProbe().getHttpGet().getPath());
            assertEquals(120, container.getStartupProbe().getFailureThreshold());
            assertEquals(params, model.getParams());
            assertEquals(Map.of(), service.getParams());
        }
    }

    @Test
    void serviceSettingsOverrideModelSettings() {
        ModelConf model = new ModelConf();
        model.setParams(Map.of("image", "custom/gbdt", "version", "v2", "pod_memory", "8Gi", "replicas", "2"));
        ServiceConf service = service(Map.of("version", "v3", "pod_memory", "16Gi", "replicas", "3"));
        Deployment deployment = deployment(new LightGBMModel().getServiceK8sYaml(model, service));
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals("custom/gbdt:v3", container.getImage());
        assertEquals("16", container.getResources().getRequests().get("memory").getAmount());
        assertEquals("Gi", container.getResources().getRequests().get("memory").getFormat());
        assertEquals(3, deployment.getSpec().getReplicas());
    }

    private static ServiceConf service(Map<String, String> params) {
        ServiceConf service = new ServiceConf();
        service.setId("gbdt-serving-test");
        service.setModelCheckpointDir("hdfs://namenode/models/v1_export");
        service.setParams(params);
        return service;
    }

    private static Deployment deployment(String yaml) {
        return Serialization.unmarshal(yaml.split("\n---")[0], Deployment.class);
    }
}
