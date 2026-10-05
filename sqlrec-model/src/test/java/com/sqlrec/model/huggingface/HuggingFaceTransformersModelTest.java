package com.sqlrec.model.huggingface;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HuggingFaceTransformersModelTest {
    private final HuggingFaceTransformersModel controller = new HuggingFaceTransformersModel();

    @Test
    void inputOptionsUseDeclaredFieldSpellingInTrainingAndServing() {
        for (String task : List.of("text-classification", "text-generation", "image-embedding")) {
            String key = task.equals("text-generation") ? "prompt_column"
                    : task.equals("image-embedding") ? "image_column" : "text_column";
            ModelConf model = model(task, "Body", "STRING");
            model.getParams().put(key, "BODY");
            if (task.equals("text-classification")) model.getParams().put("text_pair_column", "body");
            assertNull(controller.checkModel(model));
            var train = JsonParser.parseString(PipelineConfigUtils.generateTrainConfig(model, new ModelTrainConf()))
                    .getAsJsonObject().getAsJsonObject("model_params");
            var service = JsonParser.parseString(PipelineConfigUtils.generateServiceConfig(model, Map.of(key, "body")))
                    .getAsJsonObject();
            assertEquals("Body", train.get(key).getAsString());
            assertEquals("Body", service.get(key).getAsString());
            if (task.equals("text-classification")) assertEquals("Body", service.get("text_pair_column").getAsString());
            assertEquals("BODY", model.getParams().get(key));
        }
    }

    @Test
    void validatesTextGenerationAndDeclaresOutput() {
        ModelConf model = model("text-generation", "prompt", "STRING");
        model.getParams().put("prompt_column", "prompt");

        assertNull(controller.checkModel(model));
        List<FieldSchema> output = controller.getOutputFields(model);
        assertEquals(1, output.size());
        assertEquals("generated_text", output.get(0).getName());
        assertEquals("STRING", output.get(0).getType());
    }

    @Test
    void validatesEmbeddingTasks() {
        ModelConf text = model("embedding", "text", "STRING");
        text.getParams().put("text_column", "text");
        assertNull(controller.checkModel(text));
        assertEquals("ARRAY<FLOAT>", controller.getOutputFields(text).get(0).getType());

        ModelConf image = model("image-embedding", "image_url", "STRING");
        image.getParams().put("image_column", "image_url");
        assertNull(controller.checkModel(image));
        assertEquals("embedding", controller.getOutputFields(image).get(0).getName());
    }

    @Test
    void rejectsMissingOrNonStringInput() {
        ModelConf missing = model("text-classification", "body", "STRING");
        missing.getParams().put("text_column", "text");
        assertEquals("input field 'text' is not defined", controller.checkModel(missing));

        ModelConf wrongType = model("text-generation", "prompt", "BIGINT");
        wrongType.getParams().put("prompt_column", "prompt");
        assertEquals("input field 'prompt' must be STRING", controller.checkModel(wrongType));
    }

    @Test
    void generatesDownloadJobWithRevisionAndSecretReference() {
        ModelConf model = model("embedding", "text", "STRING");
        model.setPath("hdfs://namenode/user/sqlrec/models/embedding_model");
        model.getParams().put("text_column", "text");

        ModelTrainConf train = new ModelTrainConf();
        train.setId("embedding-model-v1");
        train.setCheckpointName("v1");
        train.setModelDir(model.getPath() + "/v1");
        train.setParams(new HashMap<>(Map.of(
                "revision", "abc123",
                "hf_token_secret", "hf-token",
                "hf_token_secret_key", "token"
        )));

        String yaml = controller.genModelTrainK8sYaml(model, train);
        assertTrue(yaml.contains("\"revision\": \"abc123\""));
        assertTrue(yaml.contains("name: \"HF_TOKEN\""));
        assertTrue(yaml.contains("name: \"hf-token\""));
        assertTrue(yaml.contains("$HADOOP\" fs -put"));
        assertTrue(yaml.contains("embedding_model/v1.__uploading__.embedding-model-v1"));
    }

    @Test
    void generatesReadyGpuServiceDeployment() {
        ModelConf model = model("image-embedding", "image_url", "STRING");
        model.getParams().put("image_column", "image_url");
        ServiceConf service = new ServiceConf();
        service.setId("image-embedding-service");
        service.setModelCheckpointDir("hdfs://namenode/models/image/v1");
        service.setParams(new HashMap<>(Map.of("pod_gpu", "1")));

        String yaml = controller.getServiceK8sYaml(model, service);
        assertTrue(yaml.contains("path: \"/ready\""));
        assertTrue(yaml.contains("nvidia.com/gpu"));
        assertTrue(yaml.contains("huggingface.server"));
        assertTrue(yaml.contains("hadoop fs -get"));
    }

    @Test
    void originCheckpointIsDirectlyServiceableAndExportIsUnsupported() {
        assertNull(controller.validateServiceCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN));
        assertNotNull(controller.validateServiceCheckpointType(Consts.CHECKPOINT_TYPE_EXPORT));
        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class,
                () -> controller.getExportCheckpoints(new ModelExportConf()));
        assertTrue(error.getMessage().contains("does not support model export"));
    }

    @Test
    void rejectsTaskChangesDuringTrainingAndServing() {
        ModelConf model = model("text-classification", "text", "STRING");
        model.getParams().put("text_column", "text");
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("task", "embedding"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> controller.genModelTrainK8sYaml(model, train)).getMessage().contains("cannot change the task"));
        assertThrows(IllegalArgumentException.class, () -> PipelineConfigUtils.generateTrainConfig(model, train));

        ServiceConf service = new ServiceConf();
        service.setId("hf-test");
        service.setModelCheckpointDir("hdfs://namenode/models/v1");
        service.setParams(Map.of("task", "embedding"));
        assertThrows(IllegalArgumentException.class, () -> controller.getServiceK8sYaml(model, service));
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfigUtils.generateServiceConfig(model, service.getParams()));
        assertEquals(List.of("label", "score"), controller.getOutputFields(model).stream().map(FieldSchema::getName).toList());
    }

    @Test
    void preservesEffectiveTrainingParametersAndValidatesInputOverrides() {
        ModelConf model = model("embedding", "text", "STRING");
        model.getParams().put("text_column", "text");
        ModelTrainConf train = new ModelTrainConf();
        train.setParams(Map.of("task", "embedding", "revision", "abc123", "max_length", "256"));
        var config = JsonParser.parseString(PipelineConfigUtils.generateTrainConfig(model, train)).getAsJsonObject();
        assertEquals("embedding", config.get("task").getAsString());
        assertEquals("embedding", config.getAsJsonObject("model_params").get("task").getAsString());
        assertEquals("abc123", config.getAsJsonObject("model_params").get("revision").getAsString());
        assertEquals("256", config.getAsJsonObject("model_params").get("max_length").getAsString());
        assertFalse(model.getParams().containsKey("max_length"));
        train.setParams(Map.of("text_column", "missing"));
        assertThrows(IllegalArgumentException.class, () -> PipelineConfigUtils.generateTrainConfig(model, train));
    }

    private static ModelConf model(String task, String fieldName, String fieldType) {
        ModelConf model = new ModelConf();
        model.setModelName("test_model");
        model.setInputFields(List.of(new FieldSchema(fieldName, fieldType)));
        model.setParams(new HashMap<>(Map.of(
                "model", "huggingface.transformers",
                "task", task,
                "repo_id", "org/test-model"
        )));
        return model;
    }
}
