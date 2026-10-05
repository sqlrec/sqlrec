package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RocketLaunchingModelTest {
    private final RocketLaunchingModel controller = new RocketLaunchingModel();

    private ModelConf model(boolean dense, boolean shared) {
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        if (!dense) fields.add(new FieldSchema("category", "INT"));
        fields.add(new FieldSchema("price", "DOUBLE"));
        fields.add(new FieldSchema("label", "INT"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>(Map.of("model", "tzrec.rocket_launching", "label_columns", "label",
                "booster_hidden_units", "16,8", "light_hidden_units", "8,4", "num_buckets", "32",
                "embedding_dim", "8", "batch_size", "8", "num_workers", "0", "sparse_lr", "0.01", "dense_lr", "0.01"));
        if (shared) params.put("share_hidden_units", "16");
        model.setParams(params);
        return model;
    }

    private ModelTrainConf train(Map<String, String> overrides) {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("rocket-train"); train.setModelDir("/fixture/model");
        train.setTrainDataPaths(List.of("/fixture/train.parquet")); train.setParams(overrides);
        return train;
    }

    @Test
    void nativeConfigsMatchTheFixturesUsedForTrainingAndExport() throws Exception {
        for (String name : List.of("rocket_launching", "rocket_launching_shared", "rocket_launching_dense")) {
            ModelConf model = model(name.endsWith("dense"), name.endsWith("shared"));
            assertNull(controller.checkModel(model));
            String config = controller.generateTrainConfig(model, train(Map.of()));
            assertEquals(Files.readString(Path.of("src/test/resources/tzrec", name + ".config")), config);
            assertFalse(config.contains("group_type: WIDE"));
            assertFalse(config.contains("feature_name: \"label\""));
            assertFalse(config.contains("negative_sampler"));
        }
    }

    @Test
    void validatesNativeBranchesAndInapplicableOptions() {
        for (Map<String, String> invalid : List.of(Map.of("light_hidden_units", "4,2"),
                Map.of("booster_hidden_units", "16,0"), Map.of("light_hidden_units", "8,"),
                Map.of("share_hidden_units", ""), Map.of("feature_based_distillation", "false"),
                Map.of("feature_distillation_function", "EUCLID"), Map.of("hidden_units", "8"),
                Map.of("user_features", "category"), Map.of("num_expert", "2"),
                Map.of("task.label.weight", "1"), Map.of("loss", "binary_cross_entropy"),
                Map.of("label_columns", "label,price"), Map.of("label_columns", "missing"))) {
            ModelConf model = model(false, false);
            model.getParams().putAll(invalid);
            assertNotNull(controller.checkModel(model), invalid.toString());
        }
        ModelConf model = model(false, false);
        model.getInputFields().get(2).setType("STRING");
        assertNotNull(controller.checkModel(model));
        model = model(false, false);
        model.getInputFields().get(0).setName("PROBS_LIGHT");
        assertNotNull(controller.checkModel(model));
    }

    @Test
    void lifecycleKeepsOneLightOutputAndFreezesModelStructure() {
        ModelConf model = model(false, true);
        ModelExportConf export = new ModelExportConf();
        export.setId("rocket-export"); export.setBaseModelDir("/fixture/model"); export.setCheckpointName("v1");
        ServiceConf service = new ServiceConf();
        service.setId("rocket-service"); service.setModelCheckpointDir("/fixture/model_export");
        for (Map<String, String> overrides : List.of(Map.of("booster_hidden_units", "32,8"),
                Map.of("share_hidden_units", "8"), Map.of("light_hidden_units", "8,2"),
                Map.of("column.category.bucket_size", "64"), Map.of("label_columns", "price"),
                Map.of("model", "tzrec.dssm"), Map.of("feature_based_distillation", "false"))) {
            export.setParams(overrides); service.setParams(overrides);
            assertThrows(IllegalArgumentException.class, () -> controller.genModelTrainK8sYaml(model, train(overrides)));
            assertThrows(IllegalArgumentException.class, () -> controller.genModelExportK8sYaml(model, export));
            assertThrows(IllegalArgumentException.class, () -> controller.getServiceK8sYaml(model, service));
        }
        Map<String, String> runtime = Map.of("batch_size", "16", "dense_lr", "0.02", "booster_hidden_units", "16, 8");
        assertTrue(controller.genModelTrainK8sYaml(model, train(runtime)).contains("batch_size: 16"));
        export.setParams(Map.of("batch_size", "16"));
        assertTrue(controller.genModelExportK8sYaml(model, export).contains("rocket_launching {"));
        service.setParams(Map.of("replicas", "2"));
        assertTrue(controller.getServiceK8sYaml(model, service).contains("replicas: 2"));
        assertEquals(List.of("v1_export"), controller.getExportCheckpoints(export));
        assertEquals(List.of("probs_light"), controller.getOutputFields(model).stream().map(FieldSchema::getName).toList());
        assertTrue(ServiceLoader.load(ModelController.class).stream().anyMatch(provider -> provider.type().equals(RocketLaunchingModel.class)));
    }
}
