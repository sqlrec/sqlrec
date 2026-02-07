package com.sqlrec.model.tzrec;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.util.Map;

public class K8sYamlUtils {
    public static String createConfigMapYaml(String name, Map<String, String> files) {
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withData(files)
                .build();

        return Serialization.asYaml(configMap);
    }

    public static String createJobYaml(String jobName, String configMapName) {
        String image = Config.IMAGE.getDefaultValue() + ":" + Config.VERSION.getDefaultValue();

        Job job = new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                .endMetadata()
                .withNewSpec()
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("tzrec-job")
                                .withImage(image)
                                .withCommand("sh", "-c", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .addNewVolumeMount()
                                    .withName("config-volume")
                                    .withMountPath(Config.SHELL_DIR)
                                .endVolumeMount()
                            .endContainer()
                            .addNewVolume()
                                .withName("config-volume")
                                .withNewConfigMap()
                                    .withName(configMapName)
                                .endConfigMap()
                            .endVolume()
                            .withRestartPolicy("Never")
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return Serialization.asYaml(job);
    }

    public static String mergeK8sYamls(String... yamls) {
        StringBuilder mergedYaml = new StringBuilder();
        for (int i = 0; i < yamls.length; i++) {
            if (i > 0 && !yamls[i].startsWith("---")) {
                mergedYaml.append("---\n");
            }
            mergedYaml.append(yamls[i]);
            if (!yamls[i].endsWith("\n")) {
                mergedYaml.append("\n");
            }
        }
        return mergedYaml.toString();
    }
}
