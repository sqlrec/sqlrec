package com.sqlrec.k8s;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class K8sPodConfigTest {
    private final List<ConfigOption<String>> options = List.of(ModelConfigs.NAMESPACE, ModelConfigs.JAVA_HOME,
            ModelConfigs.HADOOP_HOME, ModelConfigs.CLASSPATH, ModelConfigs.HADOOP_CONF_DIR,
            ModelConfigs.CLIENT_DIR, ModelConfigs.CLIENT_PVC_NAME, ModelConfigs.CLIENT_PV_NAME);
    private List<String> originalDefaults;

    @BeforeEach
    void setUp() {
        originalDefaults = options.stream().map(ConfigOption::getDefaultValue).toList();
        options.forEach(option -> option.setDefaultValue("configured"));
        ModelConfigs.CLIENT_DIR.setDefaultValue("/client");
        ModelConfigs.CLIENT_PVC_NAME.setDefaultValue("client-pvc");
        ModelConfigs.CLIENT_PV_NAME.setDefaultValue("client-data");
    }

    @AfterEach
    void tearDown() {
        for (int i = 0; i < options.size(); i++) {
            options.get(i).setDefaultValue(originalDefaults.get(i));
        }
    }

    @Test
    void injectsRuntimeSettingsIntoJobsAndDeployments() {
        for (String kind : List.of("Job", "Deployment")) {
            String yaml = workload(kind) + """
                    ---
                    apiVersion: v1
                    kind: ConfigMap
                    metadata:
                      name: settings
                    data:
                      key: value
                    """;
            Map<String, String> params = Map.of("NAMESPACE", "runtime",
                    "kubernetes.env.EXTRA", "added", "kubernetes.env.JAVA_HOME", "/custom/java",
                    "kubernetes.node.selector.pool", "workers", "kubernetes.node.selector.arch", "amd64");

            String actual = K8sYamlUtils.injectPodConfig(yaml, params);
            var resources = K8sYamlUtils.parseK8sYaml(actual);
            assertEquals(2, resources.size());
            assertEquals("existing", resources.get(0).getMetadata().getNamespace());
            var settings = assertInstanceOf(ConfigMap.class, resources.get(1));
            assertEquals("runtime", settings.getMetadata().getNamespace());
            assertEquals(Map.of("key", "value"), settings.getData());

            PodSpec pod = podSpec(actual, kind);
            assertEquals(Map.of("disk", "ssd", "pool", "workers", "arch", "amd64"), pod.getNodeSelector());
            List<EnvVar> variables = pod.getContainers().get(0).getEnv();
            assertEquals(List.of("TOKEN", "CONFIG", "POD_NAME", "CPU_LIMIT", "PLAIN"),
                    variables.subList(0, 5).stream().map(EnvVar::getName).toList());
            assertEquals("credentials", variables.get(0).getValueFrom().getSecretKeyRef().getName());
            assertEquals("settings", variables.get(1).getValueFrom().getConfigMapKeyRef().getName());
            assertEquals("metadata.name", variables.get(2).getValueFrom().getFieldRef().getFieldPath());
            assertEquals("limits.cpu", variables.get(3).getValueFrom().getResourceFieldRef().getResource());
            assertEquals("original", variables.get(4).getValue());
            var addedEnv = variables.subList(5, variables.size()).stream()
                    .collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));
            assertEquals(Map.of("EXTRA", "added", "JAVA_HOME", "/custom/java",
                    "HADOOP_HOME", ModelConfigs.HADOOP_HOME.getValue(),
                    "CLASSPATH", ModelConfigs.CLASSPATH.getValue(),
                    "HADOOP_CONF_DIR", ModelConfigs.HADOOP_CONF_DIR.getValue(),
                    "CLIENT_DIR", ModelConfigs.CLIENT_DIR.getValue()), addedEnv);

            assertEquals(1, pod.getVolumes().size());
            var volume = pod.getVolumes().get(0);
            assertEquals(ModelConfigs.CLIENT_PV_NAME.getValue(), volume.getName());
            assertEquals(ModelConfigs.CLIENT_PVC_NAME.getValue(), volume.getPersistentVolumeClaim().getClaimName());
            var mounts = pod.getContainers().get(0).getVolumeMounts();
            assertEquals(1, mounts.size());
            assertEquals(volume.getName(), mounts.get(0).getName());
            assertEquals(ModelConfigs.CLIENT_DIR.getValue(), mounts.get(0).getMountPath());
            assertNull(mounts.get(0).getSubPath());

            String overridden = K8sYamlUtils.injectPodConfig(actual, Map.of("kubernetes.env.TOKEN", "override"));
            var token = podSpec(overridden, kind).getContainers().get(0).getEnv().get(0);
            assertEquals("TOKEN", token.getName());
            assertEquals("override", token.getValue());
            assertNull(token.getValueFrom());
        }
    }

    @Test
    void preservesExistingVolumeAndAddsOnlyMissingMounts() {
        String volumeName = ModelConfigs.CLIENT_PV_NAME.getValue();
        String yaml = """
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: existing-volume
                spec:
                  template:
                    spec:
                      volumes:
                      - name: "%s"
                        persistentVolumeClaim:
                          claimName: original-pvc
                      containers:
                      - name: main
                        image: busybox
                        volumeMounts:
                        - name: "%s"
                          mountPath: /original
                          subPath: original-subdir
                      - name: sidecar
                        image: busybox
                """.formatted(volumeName, volumeName);
        Map<String, String> params = Map.of("NAMESPACE", "runtime");
        String actual = K8sYamlUtils.injectPodConfig(yaml, params);
        var job = K8sYamlUtils.parseK8sYamlAndGetJobs(actual).get(0);
        assertEquals("runtime", job.getMetadata().getNamespace());
        PodSpec pod = job.getSpec().getTemplate().getSpec();
        assertEquals(1, pod.getVolumes().size());
        assertEquals("original-pvc", pod.getVolumes().get(0).getPersistentVolumeClaim().getClaimName());
        var mainMounts = pod.getContainers().get(0).getVolumeMounts();
        assertEquals(1, mainMounts.size());
        assertEquals("/original", mainMounts.get(0).getMountPath());
        assertEquals("original-subdir", mainMounts.get(0).getSubPath());
        var sidecarMounts = pod.getContainers().get(1).getVolumeMounts();
        assertEquals(1, sidecarMounts.size());
        assertEquals(volumeName, sidecarMounts.get(0).getName());
        assertEquals(ModelConfigs.CLIENT_DIR.getValue(), sidecarMounts.get(0).getMountPath());
        assertEquals(K8sYamlUtils.parseK8sYaml(actual),
                K8sYamlUtils.parseK8sYaml(K8sYamlUtils.injectPodConfig(actual, params)));
    }

    @Test
    @SilenceLoggers(K8sYamlUtils.class)
    void invalidAndEmptyYamlKeepTheirOriginalValues() {
        String invalid = "apiVersion: [unterminated";
        assertEquals(invalid, K8sYamlUtils.injectPodConfig(invalid, Map.of()));
        assertNull(K8sYamlUtils.injectPodConfig(null, Map.of()));
        assertEquals("", K8sYamlUtils.injectPodConfig("", Map.of()));
    }

    @Test
    @SilenceLoggers(K8sYamlUtils.class)
    void volumeInjectionFailureDoesNotPreventOtherPodSettings() {
        var job = K8sYamlUtils.parseK8sYamlAndGetJobs(workload("Job")).get(0);
        job.getSpec().getTemplate().getSpec().setContainers(Collections.singletonList(null));
        String actual = K8sYamlUtils.injectPodConfig(Serialization.asYaml(job),
                Map.of("kubernetes.node.selector.pool", "workers"));
        assertEquals("workers", podSpec(actual, "Job").getNodeSelector().get("pool"));
    }

    private static PodSpec podSpec(String yaml, String kind) {
        return kind.equals("Job")
                ? K8sYamlUtils.parseK8sYamlAndGetJobs(yaml).get(0).getSpec().getTemplate().getSpec()
                : K8sYamlUtils.parseK8sYamlAndGetDeployments(yaml).get(0).getSpec().getTemplate().getSpec();
    }

    private static String workload(String kind) {
        return """
                apiVersion: %s
                kind: %s
                metadata:
                  name: workload
                  namespace: existing
                spec:
                  template:
                    spec:
                      nodeSelector:
                        disk: ssd
                        arch: arm64
                      containers:
                      - name: main
                        image: busybox
                        env:
                        - name: TOKEN
                          valueFrom:
                            secretKeyRef:
                              name: credentials
                              key: token
                        - name: CONFIG
                          valueFrom:
                            configMapKeyRef:
                              name: settings
                              key: config
                        - name: POD_NAME
                          valueFrom:
                            fieldRef:
                              fieldPath: metadata.name
                        - name: CPU_LIMIT
                          valueFrom:
                            resourceFieldRef:
                              resource: limits.cpu
                        - name: PLAIN
                          value: original
                """.formatted(kind.equals("Job") ? "batch/v1" : "apps/v1", kind);
    }
}
