package com.sqlrec.model.external;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ExternalModelTest {

    @Test
    public void testGetModelName() {
        ExternalModel modelController = new ExternalModel();
        assertEquals("external", modelController.getModelName());
    }

    @Test
    public void testGetOutputFieldsWithEmptyParams() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        model.setParams(new HashMap<>());

        assertThrows(IllegalArgumentException.class, () -> {
            modelController.getOutputFields(model);
        });
    }

    @Test
    public void testGetOutputFieldsWithSingleField() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        Map<String, String> params = new HashMap<>();
        params.put("output_columns", "score:float");
        model.setParams(params);

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(1, outputFields.size());
        assertEquals("score", outputFields.get(0).getName());
        assertEquals("float", outputFields.get(0).getType());
    }

    @Test
    public void testGetOutputFieldsWithMultipleFields() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        Map<String, String> params = new HashMap<>();
        params.put("output_columns", "score:float,category:string,count:int");
        model.setParams(params);

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(3, outputFields.size());
        assertEquals("score", outputFields.get(0).getName());
        assertEquals("float", outputFields.get(0).getType());
        assertEquals("category", outputFields.get(1).getName());
        assertEquals("string", outputFields.get(1).getType());
        assertEquals("count", outputFields.get(2).getName());
        assertEquals("int", outputFields.get(2).getType());
    }

    @Test
    public void testGetOutputFieldsWithSpaces() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        Map<String, String> params = new HashMap<>();
        params.put("output_columns", " score : float , category : string ");
        model.setParams(params);

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(2, outputFields.size());
        assertEquals("score", outputFields.get(0).getName());
        assertEquals("float", outputFields.get(0).getType());
        assertEquals("category", outputFields.get(1).getName());
        assertEquals("string", outputFields.get(1).getType());
    }

    @Test
    public void testGetOutputFieldsWithInvalidFormat() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        Map<String, String> params = new HashMap<>();
        params.put("output_columns", "score,category");
        model.setParams(params);

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertTrue(outputFields.isEmpty());
    }

    @Test
    public void testGetOutputFieldsWithMixedFormat() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        Map<String, String> params = new HashMap<>();
        params.put("output_columns", "score:float,invalid_field,category:string");
        model.setParams(params);

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(2, outputFields.size());
        assertEquals("score", outputFields.get(0).getName());
        assertEquals("float", outputFields.get(0).getType());
        assertEquals("category", outputFields.get(1).getName());
        assertEquals("string", outputFields.get(1).getType());
    }

    @Test
    public void testCheckModel() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();

        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testGenModelTrainK8sYamlThrowsException() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ModelTrainConf trainConf = new ModelTrainConf();

        assertThrows(UnsupportedOperationException.class, () -> {
            modelController.genModelTrainK8sYaml(model, trainConf);
        });

        try {
            modelController.genModelTrainK8sYaml(model, trainConf);
            fail("Expected UnsupportedOperationException to be thrown");
        } catch (UnsupportedOperationException e) {
            assertEquals("External model does not support training", e.getMessage());
        }
    }

    @Test
    public void testGetExportCheckpointsThrowsException() {
        ExternalModel modelController = new ExternalModel();
        ModelExportConf exportConf = new ModelExportConf();

        assertThrows(UnsupportedOperationException.class, () -> {
            modelController.getExportCheckpoints(exportConf);
        });

        try {
            modelController.getExportCheckpoints(exportConf);
            fail("Expected UnsupportedOperationException to be thrown");
        } catch (UnsupportedOperationException e) {
            assertEquals("External model does not support export", e.getMessage());
        }
    }

    @Test
    public void testGenModelExportK8sYamlThrowsException() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ModelExportConf exportConf = new ModelExportConf();

        assertThrows(UnsupportedOperationException.class, () -> {
            modelController.genModelExportK8sYaml(model, exportConf);
        });

        try {
            modelController.genModelExportK8sYaml(model, exportConf);
            fail("Expected UnsupportedOperationException to be thrown");
        } catch (UnsupportedOperationException e) {
            assertEquals("External model does not support export", e.getMessage());
        }
    }

    @Test
    public void testGetServiceUrl() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();

        Map<String, String> params = new HashMap<>();
        params.put("url", "http://external-service.example.com:8080/predict");
        serviceConf.setParams(params);

        String url = modelController.getServiceUrl(model, serviceConf);

        assertNotNull(url);
        assertEquals("http://external-service.example.com:8080/predict", url);
    }

    @Test
    public void testGetServiceUrlWithNullParams() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();

        assertThrows(IllegalArgumentException.class, () -> {
            modelController.getServiceUrl(model, serviceConf);
        });
    }

    @Test
    public void testGetServiceUrlWithEmptyParams() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setParams(new HashMap<>());

        assertThrows(IllegalArgumentException.class, () -> {
            modelController.getServiceUrl(model, serviceConf);
        });
    }

    @Test
    public void testGetServiceK8sYaml() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();

        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        assertNotNull(k8sYaml);
        assertEquals("", k8sYaml);
    }

    @Test
    public void testGetServiceK8sYamlWithParams() {
        ExternalModel modelController = new ExternalModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();

        Map<String, String> params = new HashMap<>();
        params.put("url", "http://external-service.example.com:8080/predict");
        serviceConf.setParams(params);

        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        assertNotNull(k8sYaml);
        assertEquals("", k8sYaml);
    }
}
