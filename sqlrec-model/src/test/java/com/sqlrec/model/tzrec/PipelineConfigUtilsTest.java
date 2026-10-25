package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigUtilsTest {
    private ModelConf model(String labelType) {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("iid", "STRING"),
                new FieldSchema("price", "DOUBLE"), new FieldSchema("label", labelType)));
        model.setParams(new HashMap<>(Map.of("label_columns", "label")));
        return model;
    }


    @Test
    void poolSamplingUsesTheTrainingSourceAndNativeDssmPath() {
        ModelConf model = model("INT");
        model.getParams().put("model", "tzrec.dssm");
        model.getParams().put("item_features", "iid,price");
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("negative_sampling", "pool", "item_id_column", "iid", "batch_size", "256"));
        train.setTrainDataPaths(List.of("jfs://myjfs/train/*"));
        String config = PipelineConfigUtils.generateDSSMTrainConfig(model, train);
        assertTrue(config.contains("input_path: \"jfs://myjfs/train/*.parquet\""));
        assertTrue(config.contains("pool_size: 4096"));
        assertTrue(config.contains("attr_fields: \"price\""));
        assertFalse(config.contains("attr_fields: \"uid\""));
        assertTrue(config.contains("in_batch_negative: false"));
        assertTrue(config.contains("softmax_cross_entropy {}"));
        assertFalse(config.contains("logq"));
    }


    @Test
    void integerAliasUsesDirectBucketsRatherThanHashing() {
        for (String type : List.of("INT", "INTEGER", "BIGINT")) {
            ModelConf model = model("INTEGER");
            model.setInputFields(List.of(new FieldSchema("uid", type), new FieldSchema("label", "INTEGER")));
            model.getParams().put("num_buckets", "100");
            assertNull(new WideAndDeepModel().checkModel(model));
            String config = PipelineConfigUtils.generateFeatureConfigs(model);
            assertTrue(config.contains("num_buckets: 100"), config);
            assertFalse(config.contains("hash_bucket_size"), config);
        }
    }



    @Test
    void directoryGlobsSelectParquetForTrainingEvaluationAndExport() {
        ModelConf model = model("INT");
        List<String> paths = List.of("hdfs://namenode:9000/train/dt=2026-10-05/*", "/fixture/train/*",
                "/fixture/train/part-00000", "/fixture/train/*.parquet", "/fixture/part-*.parquet");
        String expectedInput = "train_input_path: \"hdfs://namenode:9000/train/dt=2026-10-05/*.parquet,"
                + "/fixture/train/*.parquet,/fixture/train/part-00000,/fixture/train/*.parquet,/fixture/part-*.parquet\"\n";
        ModelTrainConf train = new ModelTrainConf();
        train.setModelDir("/fixture/model"); train.setTrainDataPaths(paths);
        train.setParams(Map.of("eval_input_path", "hdfs://namenode:9000/eval/*,/fixture/eval/*.parquet,/fixture/eval/part-00000"));
        String trainConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train);
        assertTrue(trainConfig.startsWith(expectedInput));
        assertTrue(trainConfig.contains("eval_input_path: \"hdfs://namenode:9000/eval/*.parquet,"
                + "/fixture/eval/*.parquet,/fixture/eval/part-00000\"\n"));

        ModelExportConf export = new ModelExportConf();
        export.setBaseModelDir("/fixture/model"); export.setTrainDataPaths(paths); export.setParams(Map.of());
        assertTrue(PipelineConfigUtils.generateWideAndDeepExportConfig(model, export).startsWith(expectedInput));
        assertEquals(paths, train.getTrainDataPaths());
        assertEquals(paths, export.getTrainDataPaths());
    }

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
    void dssmKeepsRecallConfigurationAndAcceptsExplicitSoftmax() {
        ModelConf model = model("INT");
        model.getParams().put("item_features", "iid");
        String recall = PipelineConfigUtils.generateDSSMModelConfig(model);
        assertTrue(recall.contains("in_batch_negative: true"));
        assertTrue(recall.contains("softmax_cross_entropy {}"));
        assertTrue(recall.contains("recall_at_k"));
        assertFalse(recall.contains("feature_name: \"label\""));
        model.getParams().put("loss", "softmax_cross_entropy");
        assertNull(new DSSMModel().checkModel(model));
        assertEquals(recall, PipelineConfigUtils.generateDSSMModelConfig(model));
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
    void invalidFeaturesAndNumericSettingsFailBeforeJobsAreCreated() {
        for (Map<String, String> invalid : List.of(Map.of("num_buckets", "0"), Map.of("embedding_dim", "-1"),
                Map.of("label_columns", "a,b"), Map.of("column.price.normalizer", "method=zscore,mean=1,standard_deviation=0"),
                Map.of("column.price.boundaries", "2,1"), Map.of("column.price.default_value", "NaN"),
                Map.of("column.price.autodis.num_channels", "3"),
                Map.of("hidden_units", "16,0"))) {
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
