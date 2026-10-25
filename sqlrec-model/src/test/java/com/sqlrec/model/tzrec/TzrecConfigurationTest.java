package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TzrecConfigurationTest {
    private ModelConf model(String architecture) {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("iid", "INT"),
                new FieldSchema("label", "INT")));
        model.setParams(new HashMap<>(Map.of("model", architecture, "label_columns", "label",
                "user_features", "uid", "item_features", "iid")));
        return model;
    }

    private ModelTrainConf train(Map<String, String> params) {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("stage-test");
        train.setModelDir("/tmp/stage/v1");
        train.setTrainDataPaths(List.of("/data/train/*.parquet"));
        train.setParams(params);
        return train;
    }

    private ConfigMap configMap(String yaml) {
        for (Object resource : (Iterable<?>) Serialization.unmarshal(yaml)) {
            if (resource instanceof ConfigMap configMap) return configMap;
        }
        throw new AssertionError("Missing ConfigMap");
    }

    private Job job(String yaml) {
        for (Object resource : (Iterable<?>) Serialization.unmarshal(yaml)) {
            if (resource instanceof Job job) return job;
        }
        throw new AssertionError("Missing Job");
    }

    @Test
    void consumedTrainingAndRuntimeValuesAreStillValidated() {
        WideAndDeepModel controller = new WideAndDeepModel();
        for (Map<String, String> invalid : List.of(Map.of("batch_size", "0"), Map.of("num_workers", "-1"),
                Map.of("sparse_lr", "NaN"), Map.of("dense_lr", "0"), Map.of("mixed_precision", "bad"),
                Map.of("num_epochs", "0"), Map.of("master_port", "65536"), Map.of("random_seed", "-1"),
                Map.of("pod_cpu_cores", "0"), Map.of("pod_memory_limit", "1Gi"), Map.of("pod_cpu_limit", "500m"))) {
            assertThrows(IllegalArgumentException.class,
                    () -> controller.genModelTrainK8sYaml(model("tzrec.wide_and_deep"), train(invalid)), invalid.toString());
        }
    }

    @Test
    void trainCanOverrideModelTaskWeightsAndMetrics() {
        ModelConf model = model("tzrec.mmoe");
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("label", "INT"),
                new FieldSchema("watch", "DOUBLE")));
        model.getParams().putAll(Map.of("label_columns", "label,watch", "task.watch.type", "regression",
                "task.watch.weight", "bad", "task.watch.metrics", "bad"));
        MMoEModel controller = new MMoEModel();
        assertNull(controller.checkModel(model));
        model.getParams().putAll(Map.of("task.watch.weight", "0.4", "task.watch.metrics", "mean_squared_error"));
        String inherited = configMap(controller.genModelTrainK8sYaml(model, train(Map.of())))
                .getData().get("pipeline.config");
        assertTrue(inherited.contains("weight: 0.4"));
        assertTrue(inherited.contains("metrics { mean_squared_error {} }"));
        String pipeline = configMap(controller.genModelTrainK8sYaml(model,
                train(Map.of("task.watch.weight", "0.25", "task.watch.metrics", "mean_absolute_error"))))
                .getData().get("pipeline.config");
        assertTrue(pipeline.contains("weight: 0.25"));
        assertTrue(pipeline.contains("metrics { mean_absolute_error {} }"));
        for (Map<String, String> invalid : List.of(Map.of("task.watch.weight", "NaN"), Map.of("task.watch.metrics", "auc"))) {
            assertThrows(IllegalArgumentException.class, () -> controller.genModelTrainK8sYaml(model, train(invalid)));
        }
    }

    @Test
    void modelProvidesTrainingDefaultsAndTrainOverridesThem() {
        ModelConf model = model("tzrec.wide_and_deep");
        model.getParams().putAll(Map.of("batch_size", "64", "num_workers", "0", "num_epochs", "3",
                "sparse_lr", "0.03", "dense_lr", "0.02", "pod_cpu_cores", "2", "unused", "bad"));
        WideAndDeepModel controller = new WideAndDeepModel();
        assertNull(controller.checkModel(model));
        String inherited = controller.genModelTrainK8sYaml(model, train(Map.of()));
        String pipeline = configMap(inherited).getData().get("pipeline.config");
        assertTrue(pipeline.contains("batch_size: 64"));
        assertTrue(pipeline.contains("num_workers: 0"));
        assertTrue(pipeline.contains("num_epochs: 3"));
        assertTrue(pipeline.contains("lr: 0.03"));
        assertEquals("2", job(inherited).getSpec().getTemplate().getSpec().getContainers().get(0)
                .getResources().getRequests().get("cpu").getAmount());
        String overridden = controller.genModelTrainK8sYaml(model, train(Map.of("batch_size", "32",
                "num_epochs", "2", "sparse_lr", "0.04", "pod_cpu_cores", "3")));
        pipeline = configMap(overridden).getData().get("pipeline.config");
        assertTrue(pipeline.contains("batch_size: 32"));
        assertTrue(pipeline.contains("num_epochs: 2"));
        assertTrue(pipeline.contains("lr: 0.04"));
        assertTrue(pipeline.contains("lr: 0.02"));
        assertEquals("3", job(overridden).getSpec().getTemplate().getSpec().getContainers().get(0)
                .getResources().getRequests().get("cpu").getAmount());
        assertEquals("64", model.getParams().get("batch_size"));
    }

    @Test
    void unknownAndWrongStageParametersAreIgnoredWithoutChangingStructure() {
        ModelConf model = model("tzrec.wide_and_deep");
        WideAndDeepModel controller = new WideAndDeepModel();
        ModelTrainConf train = train(Map.of("batch_size", "32", "hidden_units", "bad", "embedding_dim", "bad",
                "model", "unknown", "batch_szie", "bad", "task.missing.weight", "bad"));
        String pipeline = configMap(controller.genModelTrainK8sYaml(model, train)).getData().get("pipeline.config");
        assertTrue(pipeline.contains("batch_size: 32"));
        assertTrue(pipeline.contains("hidden_units: [512,256,128]"));
        assertTrue(pipeline.contains("embedding_dim: 16"));
        assertFalse(pipeline.contains("bad"));
        assertDoesNotThrow(() -> controller.genModelTrainK8sYaml(model,
                train(Map.of("negative_sampling", "pool", "item_id_column", "missing"))));
    }

    @Test
    void poolConstraintsAreCheckedOnlyForTheTrainingRecipe() {
        ModelConf model = model("tzrec.dssm");
        model.getParams().putAll(Map.of("negative_sampling", "pool", "negative_pool_size", "2"));
        DSSMModel controller = new DSSMModel();
        assertNull(controller.checkModel(model));
        Map<String, String> params = new HashMap<>(Map.of("batch_size", "8",
                "negative_pool_size", "32", "item_id_column", "iid"));
        String pipeline = configMap(controller.genModelTrainK8sYaml(model, train(params))).getData().get("pipeline.config");
        assertTrue(pipeline.contains("pool_negative_sampler"));
        String sampler = pipeline.substring(pipeline.indexOf("    pool_negative_sampler {"), pipeline.indexOf("feature_configs"));
        assertTrue(sampler.contains("attr_fields: \"iid\""));
        assertFalse(sampler.contains("attr_fields: \"uid\""));
        model.getParams().remove("item_features");
        String inferred = configMap(controller.genModelTrainK8sYaml(model, train(params))).getData().get("pipeline.config");
        String inferredSampler = inferred.substring(inferred.indexOf("    pool_negative_sampler {"), inferred.indexOf("feature_configs"));
        assertTrue(inferredSampler.contains("attr_fields: \"iid\""));
        assertFalse(inferredSampler.contains("attr_fields: \"uid\""));
        for (Map<String, String> invalid : List.of(Map.of("negative_pool_size", "8"), Map.of("negative_sample_count", "0"),
                Map.of("item_id_column", "uid"), Map.of("negative_sampling", "bad"))) {
            Map<String, String> candidate = new HashMap<>(params);
            candidate.putAll(invalid);
            assertThrows(IllegalArgumentException.class, () -> controller.genModelTrainK8sYaml(model, train(candidate)));
        }
        model.getParams().put("item_features", "uid,iid");
        assertThrows(IllegalArgumentException.class, () -> controller.genModelTrainK8sYaml(model, train(params)));
    }

    @Test
    void exportHasItsOwnBatchResourcesAndFixedTopology() {
        ModelExportConf export = new ModelExportConf();
        export.setId("stage-export"); export.setBaseModelDir("/tmp/stage/v1");
        export.setParams(Map.of("batch_size", "17", "pod_cpu_cores", "2",
                "num_epochs", "bad", "num_workers", "bad", "nnodes", "bad"));
        String yaml = new WideAndDeepModel().genModelExportK8sYaml(model("tzrec.wide_and_deep"), export);
        String pipeline = configMap(yaml).getData().get("pipeline.config");
        assertTrue(pipeline.contains("batch_size: 17"));
        assertTrue(pipeline.contains("num_workers: 1"));
        assertFalse(pipeline.contains("train_config"));
        assertEquals(1, job(yaml).getSpec().getCompletions());
        ModelConf defaults = model("tzrec.wide_and_deep");
        defaults.getParams().put("batch_size", "23");
        export.setParams(Map.of());
        assertTrue(configMap(new WideAndDeepModel().genModelExportK8sYaml(defaults, export))
                .getData().get("pipeline.config").contains("batch_size: 23"));
        export.setParams(Map.of("batch_size", "0"));
        assertThrows(IllegalArgumentException.class,
                () -> new WideAndDeepModel().genModelExportK8sYaml(model("tzrec.wide_and_deep"), export));
    }

    @Test
    void serviceInheritsModelResourcesAndAllowsOverrides() {
        ModelConf model = model("tzrec.wide_and_deep");
        model.getParams().putAll(Map.of("image", "unused/model", "replicas", "8", "pod_cpu_cores", "9", "NAMESPACE", "model-ns"));
        ServiceConf service = new ServiceConf();
        service.setId("stage-service"); service.setModelCheckpointDir("/tmp/stage/v1_export");
        service.setParams(Map.of("replicas", "2", "pod_cpu_cores", "3", "image", "runtime/service", "unused", "bad"));
        WideAndDeepModel controller = new WideAndDeepModel();
        assertTrue(controller.getServiceUrl(model, service).contains(".model-ns.svc."));
        String yaml = controller.getServiceK8sYaml(model, service);
        assertTrue(yaml.contains("replicas: 2"));
        assertTrue(yaml.contains("cpu: \"3\""));
        assertFalse(yaml.contains("unused/model"));
        assertTrue(yaml.contains("runtime/service:"));
        assertFalse(yaml.contains("TORCH_MANUAL_SEED"));
    }

    @Test
    void rawEnvironmentCannotOverrideGeneratedTopologyOrSeeds() {
        ModelTrainConf train = train(Map.of("nnodes", "2", "random_seed", "123", "kubernetes.env.NNODES", "1",
                "kubernetes.env.TORCH_MANUAL_SEED", "9", "kubernetes.env.CUSTOM", "yes"));
        Job job = job(new WideAndDeepModel().genModelTrainK8sYaml(model("tzrec.wide_and_deep"), train));
        assertEquals(2, job.getSpec().getCompletions());
        var env = job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(env.stream().anyMatch(value -> value.getName().equals("NNODES") && value.getValue().equals("2")));
        assertTrue(env.stream().anyMatch(value -> value.getName().equals("TORCH_MANUAL_SEED") && value.getValue().equals("123")));
        assertFalse(train.getParams().containsKey("kubernetes.env.NNODES"));
        assertEquals("yes", train.getParams().get("kubernetes.env.CUSTOM"));
    }

}
