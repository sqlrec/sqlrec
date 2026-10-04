package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigUtilsTest {
    @Test
    void floatDefaultsAndNormalizerNumbersUseTheServingDecimalContract() {
        for (String value : List.of("1f", "0x1.0p0", "3.4028235e38")) {
            ModelConf model = model("INT");
            model.getParams().put("column.price.default_value", value);
            assertNotNull(new WideAndDeepModel().checkModel(model), value);
            model.getParams().remove("column.price.default_value");
            model.getParams().put("column.price.boundaries", value);
            assertNotNull(new WideAndDeepModel().checkModel(model), value);
        }
        for (String normalizer : List.of("method=zscore,mean=1f,standard_deviation=1",
                "method=zscore,mean=0x1.0p0,standard_deviation=1",
                "method=minmax,min=-3e38,max=3e38")) {
            ModelConf model = model("INT");
            model.getParams().put("column.price.normalizer", normalizer);
            assertNotNull(new WideAndDeepModel().checkModel(model), normalizer);
        }
    }

    @Test
    void exportUsesOneProcessEvenWhenTrainingIsDistributed() {
        ModelConf model = model("INT");
        model.getParams().putAll(Map.of("nnodes", "2", "nproc_per_node", "2"));
        ModelExportConf export = new ModelExportConf();
        export.setId("export"); export.setBaseModelDir("/tmp/train"); export.setParams(Map.of());
        String yaml = new WideAndDeepModel().genModelExportK8sYaml(model, export);
        io.fabric8.kubernetes.api.model.batch.v1.Job job = Arrays.stream(yaml.split("\n---\n"))
                .map(part -> Serialization.unmarshal(part))
                .filter(resource -> resource instanceof io.fabric8.kubernetes.api.model.batch.v1.Job)
                .map(resource -> (io.fabric8.kubernetes.api.model.batch.v1.Job) resource).findFirst().orElseThrow();
        var container = job.getSpec().getTemplate().getSpec().getContainers().get(0);
        Map<String, String> env = new HashMap<>();
        container.getEnv().forEach(value -> env.put(value.getName(), value.getValue()));
        assertEquals("1", env.get("NNODES"));
        assertEquals("1", env.get("NPROC_PER_NODE"));
        export.setParams(Map.of("nproc_per_node", "2"));
        assertThrows(IllegalArgumentException.class, () -> new WideAndDeepModel().genModelExportK8sYaml(model, export));
    }

    private ModelConf model(String labelType) {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("iid", "STRING"),
                new FieldSchema("price", "DOUBLE"), new FieldSchema("label", labelType)));
        model.setParams(new HashMap<>(Map.of("label_columns", "label")));
        return model;
    }

    @Test
    void labelsAreExcludedBeforeTypeSelectionAndFloatsArePreserved() {
        for (String type : List.of("INT", "BIGINT", "FLOAT", "DOUBLE")) {
            ModelConf model = model(type);
            assertNull(new WideAndDeepModel().checkModel(model));
            String features = PipelineConfigUtils.generateFeatureConfigs(model);
            String groups = PipelineConfigUtils.generateModelConfig(model);
            assertFalse(features.contains("feature_name: \"label\""));
            assertFalse(groups.contains("feature_names: \"label\""));
            assertTrue(features.contains("raw_feature {\n        feature_name: \"price\""));
            assertTrue(groups.contains("feature_names: \"price\""));
            assertTrue(groups.contains("wide_and_deep {"));
            assertFalse(groups.contains("deepfm {"));
            assertFalse(groups.substring(0, groups.indexOf("group_name: \"deep\"")).contains("feature_names: \"price\""));
        }
    }

    @Test
    void dssmInferenceIncludesFloatsAndNeverLabels() {
        ModelConf model = model("INT");
        model.getParams().put("item_features", "iid");
        assertNull(new DSSMModel().checkModel(model));
        String groups = PipelineConfigUtils.generateDSSMModelConfig(model);
        assertTrue(groups.contains("feature_names: \"price\""));
        assertFalse(groups.contains("feature_names: \"label\""));
        for (String invalid : List.of("missing", "label", "uid,uid", "uid,iid,price")) {
            model.getParams().put("item_features", invalid);
            assertNotNull(new DSSMModel().checkModel(model), invalid);
        }
    }

    @Test
    void explicitDeepfmKeepsAnEqualDimensionSparseFmGroup() {
        ModelConf model = model("INT");
        model.getParams().put("model", "tzrec.deepfm");
        String groups = PipelineConfigUtils.generateModelConfig(model);
        assertTrue(groups.contains("deepfm {"));
        assertFalse(groups.substring(groups.indexOf("group_name: \"fm\"")).contains("feature_names: \"price\""));
        model.getParams().put("column.uid.embedding_dim", "8");
        assertNotNull(new DeepFMModel().checkModel(model));
        assertNull(new WideAndDeepModel().checkModel(model));
    }

    @Test
    void trainExportAndServiceInheritModelParamsAndOperationOverridesWin() {
        ModelConf model = model("INT");
        model.getParams().putAll(Map.of("sparse_lr", "0.25", "num_epochs", "7", "image", "custom/tzrec",
                "version", "custom", "pod_cpu_cores", "3", "USE_SPAWN_MULTI_PROCESS", "0"));
        ModelTrainConf train = new ModelTrainConf();
        train.setId("train"); train.setModelDir("/tmp/train"); train.setParams(Map.of("dense_lr", "0.2"));
        String config = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train);
        assertTrue(config.contains("lr: 0.25")); assertTrue(config.contains("lr: 0.2")); assertTrue(config.contains("num_epochs: 7"));
        String yaml = new WideAndDeepModel().genModelTrainK8sYaml(model, train);
        assertTrue(yaml.contains("custom/tzrec:custom")); assertTrue(yaml.contains("cpu: \"3\""));
        ServiceConf service = new ServiceConf();
        service.setId("service"); service.setModelCheckpointDir("/tmp/export"); service.setParams(Map.of("pod_cpu_cores", "4"));
        Deployment deployment = Serialization.unmarshal(new WideAndDeepModel().getServiceK8sYaml(model, service).split("\n---\n", 2)[0], Deployment.class);
        var container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals("custom/tzrec:custom", container.getImage());
        assertEquals("4", container.getResources().getRequests().get("cpu").getAmount());
        ModelExportConf export = new ModelExportConf();
        export.setId("export"); export.setBaseModelDir("/tmp/train"); export.setParams(Map.of("batch_size", "2"));
        String exportYaml = new WideAndDeepModel().genModelExportK8sYaml(model, export);
        assertTrue(exportYaml.contains("custom/tzrec:custom")); assertTrue(exportYaml.contains("batch_size: 2"));
        train.setParams(Map.of("batch_size", "0"));
        assertThrows(IllegalArgumentException.class, () -> new WideAndDeepModel().genModelTrainK8sYaml(model, train));
        train.setParams(Map.of("model", "tzrec.deepfm"));
        assertThrows(IllegalArgumentException.class, () -> new WideAndDeepModel().genModelTrainK8sYaml(model, train));
    }

    @Test
    void invalidFeaturesAndNumericSettingsFailBeforeJobsAreCreated() {
        for (Map<String, String> invalid : List.of(Map.of("num_buckets", "0"), Map.of("embedding_dim", "-1"),
                Map.of("label_columns", "a,b"), Map.of("column.price.normalizer", "method=zscore,mean=1,standard_deviation=0"),
                Map.of("column.price.boundaries", "2,1"), Map.of("column.price.default_value", "NaN"),
                Map.of("column.price.autodis.num_channels", "3"), Map.of("column.missing.embedding_dim", "8"),
                Map.of("hidden_units", "16,0"), Map.of("mixed_precision", "garbage"))) {
            ModelConf model = model("INT"); model.getParams().putAll(invalid);
            assertNotNull(new WideAndDeepModel().checkModel(model), invalid.toString());
        }
        ModelConf model = model("INT");
        model.setInputFields(List.of(new FieldSchema("ids", "ARRAY<INT>")));
        assertTrue(new WideAndDeepModel().checkModel(model).contains("ARRAY<STRING>"));
    }

    @Test
    void vectorDimensionsDefaultsAndTextEscapesAreExplicit() {
        ModelConf model = model("INT");
        model.setInputFields(List.of(new FieldSchema("id", "INT"), new FieldSchema("vector", "ARRAY<FLOAT>")));
        assertNotNull(new WideAndDeepModel().checkModel(model));
        model.getParams().put("column.vector.value_dim", "2");
        assertNull(new WideAndDeepModel().checkModel(model));
        String config = PipelineConfigUtils.generateFeatureConfigs(model);
        assertTrue(config.contains("value_dim: 2")); assertTrue(config.contains("default_value: \"0\\0350\""));
        model.getParams().put("column.vector.embedding", "autodis");
        assertTrue(new WideAndDeepModel().checkModel(model).contains("requires value_dim=1"));
        model.getParams().put("column.vector.embedding", "mlp");
        assertNull(new WideAndDeepModel().checkModel(model));
        assertEquals("\"a\\\"b\\\\c\\012\"", PipelineConfigUtils.quoted("a\"b\\c\n"));
    }
}
