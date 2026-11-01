package com.sqlrec.k8s;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    }

    @AfterEach
    void tearDown() {
        for (int i = 0; i < options.size(); i++) {
            options.get(i).setDefaultValue(originalDefaults.get(i));
        }
    }

    @Test
    void combinedInjectionMatchesIndividualStepsForJobsAndDeployments() {
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
                    "kubernetes.node.selector.pool", "workers");
            Map<String, String> env = new HashMap<>();
            for (var option : options.subList(1, 6)) {
                env.put(option.getKey(), option.getValue());
            }
            env.putAll(Map.of("EXTRA", "added", "JAVA_HOME", "/custom/java"));
            String expected = K8sYamlUtils.injectNamespaceIntoYaml(yaml, "runtime");
            expected = K8sYamlUtils.injectEnvVarsIntoYaml(expected, env);
            expected = K8sYamlUtils.injectVolumeMountIntoYaml(expected, ModelConfigs.CLIENT_PVC_NAME.getValue(),
                    ModelConfigs.CLIENT_PV_NAME.getValue(), ModelConfigs.CLIENT_DIR.getValue(), null);
            expected = K8sYamlUtils.injectNodeSelectorIntoYaml(expected, Map.of("pool", "workers"));

            String actual = K8sYamlUtils.injectPodConfig(yaml, new ModelConf(), params);
            assertEquals(K8sYamlUtils.parseK8sYaml(expected), K8sYamlUtils.parseK8sYaml(actual));
            var resources = K8sYamlUtils.parseK8sYaml(actual);
            assertEquals("existing", resources.get(0).getMetadata().getNamespace());
            assertEquals("runtime", resources.get(1).getMetadata().getNamespace());
            PodSpec pod = podSpec(actual, kind);
            assertEquals(Map.of("disk", "ssd", "pool", "workers"), pod.getNodeSelector());
            List<EnvVar> variables = pod.getContainers().get(0).getEnv();
            assertEquals("credentials", variables.get(0).getValueFrom().getSecretKeyRef().getName());
            assertEquals("/custom/java", variables.stream().filter(envVar -> envVar.getName().equals("JAVA_HOME"))
                    .findFirst().orElseThrow().getValue());
        }
    }

    @Test
    @SilenceLoggers(K8sYamlUtils.class)
    void invalidAndEmptyYamlKeepTheirOriginalValues() {
        String invalid = "apiVersion: [unterminated";
        assertEquals(invalid, K8sYamlUtils.injectPodConfig(invalid, new ModelConf(), Map.of()));
        assertNull(K8sYamlUtils.injectPodConfig(null, new ModelConf(), Map.of()));
        assertEquals("", K8sYamlUtils.injectPodConfig("", new ModelConf(), Map.of()));
    }

    @Test
    @SilenceLoggers(K8sYamlUtils.class)
    void volumeInjectionFailureDoesNotPreventOtherPodSettings() {
        var job = K8sYamlUtils.parseK8sYamlAndGetJobs(workload("Job")).get(0);
        job.getSpec().getTemplate().getSpec().setContainers(Collections.singletonList(null));
        String actual = K8sYamlUtils.injectPodConfig(io.fabric8.kubernetes.client.utils.Serialization.asYaml(job),
                new ModelConf(), Map.of("kubernetes.node.selector.pool", "workers"));
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
                      containers:
                      - name: main
                        image: busybox
                        env:
                        - name: TOKEN
                          valueFrom:
                            secretKeyRef:
                              name: credentials
                              key: token
                """.formatted(kind.equals("Job") ? "batch/v1" : "apps/v1", kind);
    }
}
