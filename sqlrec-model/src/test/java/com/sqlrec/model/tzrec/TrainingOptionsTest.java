package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TrainingOptionsTest {
    private ModelConf model() {
        ModelConf model = new ModelConf();
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("iid", "INT"),
                new FieldSchema("label", "INT")));
        model.setParams(new HashMap<>(Map.of("label_columns", "label")));
        return model;
    }

    @Test
    void trainingOverridesControlObservationWithoutChangingModelDefaults() {
        ModelConf model = model();
        model.getParams().put("log_step_count_steps", "50");
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("log_step_count_steps", "10", "save_checkpoints_epochs", "2",
                "use_tensorboard", "false", "enable_train_metrics", "true", "train_metric_decay_rate", "0.8"));
        String config = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train);
        assertTrue(config.contains("use_tensorboard: false"), config);
        assertTrue(config.contains("log_step_count_steps: 10"), config);
        assertTrue(config.contains("save_checkpoints_epochs: 2"), config);
        assertTrue(config.contains("save_checkpoints_steps: 0"), config);
        assertTrue(config.contains("train_metrics { auc {} decay_rate: 0.8 }"), config);
        assertFalse(config.contains("decay_step:"), config);
        assertEquals("50", model.getParams().get("log_step_count_steps"));
        assertTrue(PipelineConfigUtils.generateModelConfig(model).contains("train_metrics { auc {} }"));
    }

    @Test
    void metricsFollowRankingRetrievalAndTaskObjectives() {
        ModelConf model = model();
        model.getParams().putAll(Map.of("enable_train_metrics", "true", "log_step_count_steps", "5",
                "train_metric_decay_step", "10"));
        assertTrue(PipelineConfigUtils.generateModelConfig(model).contains("train_metrics { auc {} decay_step: 10"));
        String ranking = PipelineConfigUtils.generateModelConfig(model);
        assertTrue(ranking.contains("metrics { normalized_entropy {} }"), ranking);
        assertTrue(ranking.contains("train_metrics { accuracy {}"), ranking);
        assertFalse(ranking.contains("train_metrics { normalized_entropy"), ranking);
        assertFalse(ranking.contains("mean_squared_error"), ranking);
        model.getParams().put("model", "tzrec.deepfm");
        assertTrue(PipelineConfigUtils.generateModelConfig(model).contains("train_metrics { auc {}"));
        assertTrue(RocketLaunchingPipelineConfigUtils.modelConfig(model).contains("train_metrics { auc {}"));
        model.getParams().put("user_features", "uid");
        String retrieval = PipelineConfigUtils.generateDSSMModelConfig(model);
        assertTrue(retrieval.contains("train_metrics { recall_at_k { top_k: 1 }"), retrieval);
        assertTrue(retrieval.contains("train_metrics { recall_at_k { top_k: 5 }"), retrieval);
        assertTrue(retrieval.contains("train_metrics { recall_at_k { top_k: 10 }"), retrieval);
        assertTrue(retrieval.contains("train_metrics { recall_at_k { top_k: 20 }"), retrieval);
        assertFalse(retrieval.contains("auc"));

        model.getParams().remove("user_features");
        model.getParams().put("model", "tzrec.mmoe");
        model.getParams().putAll(Map.of("label_columns", "label,watch_time", "task.watch_time.type", "regression"));
        model.setInputFields(List.of(new FieldSchema("uid", "INT"), new FieldSchema("label", "INT"),
                new FieldSchema("watch_time", "DOUBLE")));
        String tasks = MultiTaskPipelineConfigUtils.modelConfig(model);
        assertTrue(tasks.contains("train_metrics { auc {}"), tasks);
        assertTrue(tasks.contains("train_metrics { mean_squared_error {}"), tasks);
        assertTrue(tasks.contains("train_metrics { mean_absolute_error {}"), tasks);
        assertFalse(tasks.contains("train_metrics { normalized_entropy"), tasks);
        assertTrue(tasks.indexOf("train_metrics { auc {}") < tasks.indexOf("tower_name: \"watch_time\""));
        assertTrue(tasks.indexOf("train_metrics { mean_squared_error {}") > tasks.indexOf("tower_name: \"watch_time\""));
    }

    @Test
    void trainingMetricsUseNativeDefaultsUnlessExplicitlyOverridden() {
        ModelConf model = model();
        String config = PipelineConfigUtils.generateModelConfig(model);
        assertTrue(config.contains("train_metrics { auc {} }"), config);
        assertFalse(config.contains("decay_step:"), config);
        assertFalse(config.contains("decay_rate:"), config);
        model.getParams().put("log_step_count_steps", "200");
        assertNotNull(new WideAndDeepModel().checkModel(model));
        model.getParams().put("enable_train_metrics", "false");
        assertNull(new WideAndDeepModel().checkModel(model));
        String disabled = PipelineConfigUtils.generateModelConfig(model);
        assertFalse(disabled.contains("train_metrics"), disabled);
        assertTrue(disabled.contains("metrics { normalized_entropy {} }"), disabled);
    }

    @Test
    void invalidIntervalsDecayAndBooleansFailBeforeJobSubmission() {
        for (Map<String, String> invalid : List.of(
                Map.of("use_tensorboard", "yes"), Map.of("enable_train_metrics", "1"), Map.of("is_profiling", "yes"),
                Map.of("log_step_count_steps", "0"), Map.of("log_step_count_steps", "-1"),
                Map.of("save_checkpoints_steps", "-1"), Map.of("save_checkpoints_epochs", "-1"),
                Map.of("save_checkpoints_steps", "10", "save_checkpoints_epochs", "1"),
                Map.of("train_metric_decay_step", "150", "enable_train_metrics", "true"), Map.of("train_metric_decay_step", "0"),
                Map.of("train_metric_decay_rate", "NaN"), Map.of("train_metric_decay_rate", "Infinity"),
                Map.of("train_metric_decay_rate", "-0.1"), Map.of("train_metric_decay_rate", "1"))) {
            ModelConf model = model();
            model.getParams().putAll(invalid);
            assertNotNull(new WideAndDeepModel().checkModel(model), invalid.toString());
            assertThrows(IllegalArgumentException.class,
                    () -> PipelineConfigUtils.generateTrainConfig(model, Map.of(), null), invalid.toString());
        }
    }

    @Test
    void checkpointIntervalsAllowDisabledTriggersAndPreserveTheActiveCadence() {
        record CheckpointCase(Map<String, String> params, int steps, Integer epochs) {}
        for (CheckpointCase test : List.of(
                new CheckpointCase(Map.of(), 1000, null),
                new CheckpointCase(Map.of("save_checkpoints_steps", "0"), 0, null),
                new CheckpointCase(Map.of("save_checkpoints_epochs", "0"), 1000, null),
                new CheckpointCase(Map.of("save_checkpoints_steps", "0", "save_checkpoints_epochs", "0"), 0, null),
                new CheckpointCase(Map.of("save_checkpoints_steps", "50"), 50, null),
                new CheckpointCase(Map.of("save_checkpoints_steps", "50", "save_checkpoints_epochs", "0"), 50, null),
                new CheckpointCase(Map.of("save_checkpoints_epochs", "2"), 0, 2),
                new CheckpointCase(Map.of("save_checkpoints_steps", "0", "save_checkpoints_epochs", "2"), 0, 2))) {
            ModelConf model = model();
            model.getParams().putAll(test.params());
            assertNull(new WideAndDeepModel().checkModel(model), test.params().toString());
            String config = PipelineConfigUtils.generateTrainConfig(model, Map.of(), null);
            assertTrue(config.contains("save_checkpoints_steps: " + test.steps() + "\n"), config);
            if (test.epochs() == null) assertFalse(config.contains("save_checkpoints_epochs:"), config);
            else assertTrue(config.contains("save_checkpoints_epochs: " + test.epochs() + "\n"), config);
        }
    }

    @Test
    void trainingOverridesCanSwitchCheckpointCadenceByDisablingThePreviousTrigger() {
        ModelConf model = model();
        model.getParams().put("save_checkpoints_steps", "50");
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("save_checkpoints_steps", "0", "save_checkpoints_epochs", "2"));
        String epochConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train);
        assertTrue(epochConfig.contains("save_checkpoints_steps: 0\n"), epochConfig);
        assertTrue(epochConfig.contains("save_checkpoints_epochs: 2\n"), epochConfig);
        assertEquals("50", model.getParams().get("save_checkpoints_steps"));

        model.getParams().remove("save_checkpoints_steps");
        model.getParams().put("save_checkpoints_epochs", "2");
        train.setParams(Map.of("save_checkpoints_epochs", "0", "save_checkpoints_steps", "50"));
        String stepConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train);
        assertTrue(stepConfig.contains("save_checkpoints_steps: 50\n"), stepConfig);
        assertFalse(stepConfig.contains("save_checkpoints_epochs:"), stepConfig);
        assertEquals("2", model.getParams().get("save_checkpoints_epochs"));
    }

    @Test
    void nativeDiagnosticsCanBeOverriddenWithoutChangingModelStructure() {
        ModelConf model = model();
        assertEquals(List.of("loss", "learning_rate", "global_gradient_norm"), TrainingOptions.summaries(model.getParams()));
        assertFalse(PipelineConfigUtils.generateTrainConfig(model, Map.of(), null).contains("is_profiling:"));
        Map<String, String> overrides = Map.of("tensorboard_summaries", " parameter,gradient,gradient_norm ",
                "is_profiling", "true");
        String config = PipelineConfigUtils.generateTrainConfig(model, overrides, null);
        assertTrue(config.contains("tensorboard_summaries: \"parameter\""), config);
        assertTrue(config.contains("tensorboard_summaries: \"gradient\""), config);
        assertTrue(config.contains("tensorboard_summaries: \"gradient_norm\""), config);
        assertFalse(config.contains("tensorboard_summaries: \"global_gradient_norm\""), config);
        assertTrue(config.contains("is_profiling: true"), config);
        assertEquals(List.of("loss", "learning_rate", "global_gradient_norm"), TrainingOptions.summaries(model.getParams()));
        String disabled = PipelineConfigUtils.generateTrainConfig(model,
                Map.of("tensorboard_summaries", "loss,learning_rate", "is_profiling", "false"), null);
        assertFalse(disabled.contains("global_gradient_norm"), disabled);
        assertTrue(disabled.contains("is_profiling: false"), disabled);
    }

    @Test
    void invalidSummaryTypesFailBeforeJobSubmission() {
        for (String value : List.of("", " ", "loss,", ",loss", "loss,,gradient", "loss,loss",
                "loss, loss", "unknown", "global_grad_norm", "Loss", "gradient\" {}")) {
            ModelConf model = model();
            model.getParams().put("tensorboard_summaries", value);
            assertNotNull(new WideAndDeepModel().checkModel(model), value);
            assertThrows(IllegalArgumentException.class,
                    () -> PipelineConfigUtils.generateTrainConfig(model, Map.of(), null), value);
        }
    }

    @Test
    void remoteProfilingFailsBeforeSubmittingTraining() {
        ModelConf model = model();
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("is_profiling", "true"));
        for (String path : List.of("jfs://myjfs/model", "hdfs://cluster/model", "s3://bucket/model")) {
            train.setModelDir(path);
            assertThrows(IllegalArgumentException.class,
                    () -> PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train), path);
        }
        train.setModelDir("/mnt/models/profiling");
        assertTrue(PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train).contains("is_profiling: true"));
        train.setModelDir("jfs://myjfs/model");
        train.setParams(Map.of("is_profiling", "false"));
        assertTrue(PipelineConfigUtils.generateWideAndDeepTrainConfig(model, train).contains("is_profiling: false"));
    }
}
