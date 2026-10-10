package com.sqlrec.model.gbdt;
import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
class GbdtValidationTest {
    private ModelConf model(String labels, String objective) {
        ModelConf model = new ModelConf();
        model.setParams(Map.of("label_columns", labels, "objective", objective));
        model.setInputFields(List.of(new FieldSchema("feature", "FLOAT"),
                new FieldSchema("y1", "INT"), new FieldSchema("y2", "INT")));
        return model;
    }
    @Test
    void allBackendsRejectMultipleLabelsWhitespaceAndUnsupportedObjectives() {
        for (GbdtModelBase backend : List.of(new LightGBMModel(), new XGBoostModel(), new CatBoostModel())) {
            for (String labels : List.of("y1,y2", "y1,", " y1 ", " ")) {
                assertTrue(backend.checkModel(model(labels, "binary")).contains("single column"));
            }
            assertTrue(backend.checkModel(model("y1", "multiclass")).contains("binary or regression"));
        }
    }
    @Test
    void trainingOverridesAndLegacyServicesCannotBypassTheObjectiveRestriction() {
        for (GbdtModelBase backend : List.of(new LightGBMModel(), new XGBoostModel(), new CatBoostModel())) {
            ModelConf model = model("y1", "binary");
            ModelTrainConf train = new ModelTrainConf(); train.setParams(Map.of("objective", "multiclass"));
            assertThrows(IllegalArgumentException.class, () -> backend.genModelTrainK8sYaml(model, train));
            ServiceConf service = new ServiceConf(); service.setParams(Map.of());
            ModelConf legacy = model("y1", "multiclass");
            assertThrows(IllegalArgumentException.class, () -> backend.getServiceK8sYaml(legacy, service));
        }
    }
    @Test
    void binaryAndRegressionModelsStillValidate() {
        for (GbdtModelBase backend : List.of(new LightGBMModel(), new XGBoostModel(), new CatBoostModel())) {
            for (String objective : List.of("binary", "regression")) {
                ModelConf model = model("y1", objective);
                model.setInputFields(List.of(new FieldSchema("feature", "FLOAT"), new FieldSchema("y1", "INT")));
                assertNull(backend.checkModel(model));
            }
        }
    }

    @Test
    void allOperationsRejectChangesToLabelsAndModelType() {
        for (GbdtModelBase backend : List.of(new LightGBMModel(), new XGBoostModel(), new CatBoostModel())) {
            ModelConf model = model("y1", "binary");
            for (Map<String, String> overrides : List.of(Map.of("label_columns", "y2"),
                    Map.of("label_columns", ""), Map.of("model", "tzrec.deepfm"))) {
                ModelTrainConf train = new ModelTrainConf(); train.setParams(overrides);
                ModelExportConf export = new ModelExportConf(); export.setParams(overrides);
                ServiceConf service = new ServiceConf(); service.setParams(overrides);
                assertThrows(IllegalArgumentException.class, () -> backend.genModelTrainK8sYaml(model, train));
                assertThrows(IllegalArgumentException.class, () -> backend.genModelExportK8sYaml(model, export));
                assertThrows(IllegalArgumentException.class, () -> backend.getServiceK8sYaml(model, service));
            }
        }
    }

    @Test
    void trainConfigUsesRuntimeOverridesAndAcceptsAnUnchangedContract() {
        ModelConf model = model("y1", "binary");
        model.setInputFields(List.of(new FieldSchema("feature", "FLOAT"), new FieldSchema("y1", "INT")));
        ModelTrainConf train = new ModelTrainConf();
        train.setModelDir("/model"); train.setTrainDataPaths(List.of("/train.parquet"));
        for (PipelineConfigUtils.ModelType type : PipelineConfigUtils.ModelType.values()) {
            train.setParams(Map.of("label_columns", "y1", "model", "gbdt." + type.getKey(), "learning_rate", "0.25"));
            var config = com.google.gson.JsonParser.parseString(
                    PipelineConfigUtils.generateTrainConfig(type, model, train)).getAsJsonObject();
            assertEquals("y1", config.get("label_columns").getAsString());
            assertEquals(0.25, config.getAsJsonObject("params").get("learning_rate").getAsDouble());
            assertEquals(1, config.getAsJsonArray("feature_columns").size());
            assertEquals("feature", config.getAsJsonArray("feature_columns").get(0).getAsString());
            train.setParams(Map.of("label_columns", "y2"));
            assertThrows(IllegalArgumentException.class, () -> PipelineConfigUtils.generateTrainConfig(type, model, train));
        }
        assertEquals(Map.of("label_columns", "y1", "objective", "binary"), model.getParams());
    }

}
