package com.sqlrec.model.huggingface;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HuggingFaceTransformersModelTest {
    private final HuggingFaceTransformersModel controller = new HuggingFaceTransformersModel();

    @Test
    void openAiIsAlwaysAvailableWithAServiceAliasAndSecretReference() {
        ModelConf model = model("embedding", "text", "STRING");
        model.getParams().put("text_column", "text");
        var defaults = JsonParser.parseString(PipelineConfigUtils.generateServiceConfig(model, Map.of())).getAsJsonObject();
        assertEquals("test_model", defaults.get("served_model_name").getAsString());
        assertFalse(defaults.has("enable_openai_api"));
        ServiceConf service = new ServiceConf();
        service.setId("embedding-service");
        service.setModelCheckpointDir("hdfs://namenode/models/embedding/v1");
        service.setParams(Map.of("served_model_name", "public-alias", "openai_api_key_secret", "api-key",
                "openai_api_key_secret_key", "value", "NAMESPACE", "test"));
        String yaml = controller.getServiceK8sYaml(model, service);
        assertTrue(yaml.contains("SQLREC_OPENAI_API_KEY"));
        assertTrue(yaml.contains("secretKeyRef:"));
        assertTrue(yaml.contains("name: \"api-key\""));
        assertTrue(yaml.contains("key: \"value\""));
        assertTrue(yaml.contains("public-alias"));
        assertTrue(controller.getServiceUrl(model, service).endsWith(":80/predict"));
        for (Map<String, String> invalid : List.of(Map.of("served_model_name", " "),
                Map.of("openai_max_request_bytes", "0"), Map.of("openai_max_batch_inputs", "-1"),
                Map.of("openai_api_key_secret", ""),
                Map.of("openai_api_key_secret", "api-key", "openai_api_key_secret_key", ""))) {
            assertThrows(IllegalArgumentException.class, () -> PipelineConfigUtils.generateServiceConfig(model, invalid));
        }
    }

    @Test
    void servingOnlyOptionsDoNotLeakIntoCheckpointParameters() {
        ModelConf model = model("embedding", "text", "STRING");
        model.getParams().putAll(Map.of("text_column", "text", "served_model_name", "public-alias",
                "openai_api_key_secret", "api-key", "openai_max_request_bytes", "2048"));
        var train = JsonParser.parseString(PipelineConfigUtils.generateTrainConfig(model, new ModelTrainConf()))
                .getAsJsonObject().getAsJsonObject("model_params");
        assertFalse(train.has("served_model_name"));
        assertFalse(train.has("openai_api_key_secret"));
        assertFalse(train.has("openai_max_request_bytes"));
    }

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
        for (Object resource : (Iterable<?>) Serialization.unmarshal(yaml)) {
            if (resource instanceof ConfigMap configMap) {
                assertEquals("image-embedding-service-cm", configMap.getMetadata().getName());
                assertTrue(configMap.getData().containsKey("service.config"));
                assertTrue(configMap.getData().containsKey("start.sh"));
            }
        }
        var pod = servicePodTemplate(model, service).getSpec();
        assertEquals("image-embedding-service-cm", pod.getVolumes().get(0).getConfigMap().getName());
        var container = pod.getContainers().get(0);
        assertEquals("/data", container.getVolumeMounts().get(0).getMountPath());
        assertEquals(List.of("bash", "/data/start.sh"), container.getCommand());
    }

    @Test
    void checkpointAndInferenceConfigChangesUpdateThePodTemplate() {
        ModelConf model = model("embedding", "text", "STRING");
        model.getParams().put("text_column", "text");
        ServiceConf service = new ServiceConf();
        service.setId("embedding-service");
        service.setParams(Map.of());
        service.setModelCheckpointDir("hdfs://namenode/models/embedding/v1");

        PodTemplateSpec original = servicePodTemplate(model, service);
        assertTrue(original.getMetadata().getAnnotations().get("sqlrec.io/config-checksum")
                .matches("[0-9a-f]{64}"));
        assertEquals(original, servicePodTemplate(model, service));
        service.setModelCheckpointDir("hdfs://namenode/models/embedding/v2");
        PodTemplateSpec nextCheckpoint = servicePodTemplate(model, service);
        assertNotEquals(original, nextCheckpoint);

        service.setParams(Map.of("inference_batch_size", "16"));
        PodTemplateSpec nextConfig = servicePodTemplate(model, service);
        assertNotEquals(nextCheckpoint, nextConfig);
        assertEquals(nextConfig, servicePodTemplate(model, service));
    }

    private PodTemplateSpec servicePodTemplate(ModelConf model, ServiceConf service) {
        String yaml = controller.getServiceK8sYaml(model, service);
        for (Object resource : (Iterable<?>) Serialization.unmarshal(yaml)) {
            if (resource instanceof Deployment deployment) {
                return deployment.getSpec().getTemplate();
            }
        }
        throw new AssertionError("Deployment is missing");
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
