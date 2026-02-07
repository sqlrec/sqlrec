package com.sqlrec.model.tzrec;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class K8sYamlUtilsTest {

    @Test
    public void testCreateConfigMapYamlWithSingleFile() {
        String configMapName = "test-configmap";
        Map<String, String> files = new HashMap<>();
        files.put("application.properties", "key1=value1\nkey2=value2");
        String yaml = K8sYamlUtils.createConfigMapYaml(configMapName, files);
        assertNotNull(yaml);
        System.out.println(yaml);
    }

    @Test
    public void testCreateConfigMapYamlWithMultipleFiles() {
        String configMapName = "test-configmap-multi";
        Map<String, String> files = new HashMap<>();
        files.put("application.properties", "key1=value1\nkey2=value2");
        files.put("log4j2.xml", "<Configuration><Appenders><Console name=\"Console\" target=\"SYSTEM_OUT\"/></Appenders></Configuration>");

        String yaml = K8sYamlUtils.createConfigMapYaml(configMapName, files);
        System.out.println(yaml);
    }

    @Test
    public void testCreateJobYaml() {
        String jobName = "test-job";
        String configMapName = "test-configmap";

        String yaml = K8sYamlUtils.createJobYaml(jobName, configMapName);
        System.out.println(yaml);
    }

    @Test
    public void testMergeK8sYamls() {
        String configMapName = "test-configmap";
        Map<String, String> files = new HashMap<>();
        files.put("application.properties", "key1=value1\nkey2=value2");
        String configMapYaml = K8sYamlUtils.createConfigMapYaml(configMapName, files);

        String jobName = "test-job";
        String jobYaml = K8sYamlUtils.createJobYaml(jobName, configMapName);

        String mergedYaml = K8sYamlUtils.mergeK8sYamls(configMapYaml, jobYaml);
        assertNotNull(mergedYaml);
        System.out.println(mergedYaml);
        assert mergedYaml.contains("---");
    }
} 
