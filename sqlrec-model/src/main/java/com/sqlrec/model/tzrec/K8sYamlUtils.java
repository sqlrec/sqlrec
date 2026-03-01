package com.sqlrec.model.tzrec;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.util.ArrayList;
import java.util.HashMap;
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

    public static String createHeadlessServiceYaml(String jobName, String serviceName, int masterPort) {
        HashMap<String, String> selector = new HashMap<>();
        selector.put("job-name", jobName);
        
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                .endMetadata()
                .withNewSpec()
                    .withClusterIP("None")
                    .withSelector(selector)
                    .addNewPort()
                        .withName("torch-distributed")
                        .withPort(masterPort)
                        .withNewTargetPort(masterPort)
                    .endPort()
                .endSpec()
                .build();

        return Serialization.asYaml(service);
    }

    public static String createJobYaml(String jobName, String configMapName, String serviceName, int nnodes, int nprocPerNode, int masterPort) {
        String image = Config.IMAGE.getDefaultValue() + ":" + Config.VERSION.getDefaultValue();

        Job job = new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                .endMetadata()
                .withNewSpec()
                    .withCompletions(nnodes)
                    .withParallelism(nnodes)
                    .withCompletionMode("Indexed")
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewSpec()
                            .withSubdomain(serviceName)
                            .addNewContainer()
                                .withName("tzrec-job")
                                .withImage(image)
                                .withCommand("sh", "-c", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .withEnv(
                                    new ArrayList<EnvVar>() {{
                                        add(new EnvVarBuilder().withName("JOB_NAME").withValue(jobName).build());
                                        add(new EnvVarBuilder().withName("SERVICE_NAME").withValue(serviceName).build());
                                        add(new EnvVarBuilder().withName("MASTER_PORT").withValue(String.valueOf(masterPort)).build());
                                        add(new EnvVarBuilder().withName("NNODES").withValue(String.valueOf(nnodes)).build());
                                        add(new EnvVarBuilder().withName("NPROC_PER_NODE").withValue(String.valueOf(nprocPerNode)).build());
                                        add(new EnvVarBuilder().withName("USE_FSSPEC").withValue(Config.USE_FSSPEC.getDefaultValue()).build());
                                        add(new EnvVarBuilder().withName("USE_SPAWN_MULTI_PROCESS").withValue(Config.USE_SPAWN_MULTI_PROCESS.getDefaultValue()).build());
                                        add(new EnvVarBuilder().withName("USE_FARM_HASH_TO_BUCKETIZE").withValue(Config.USE_FARM_HASH_TO_BUCKETIZE.getDefaultValue()).build());
                                    }}
                                )
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
