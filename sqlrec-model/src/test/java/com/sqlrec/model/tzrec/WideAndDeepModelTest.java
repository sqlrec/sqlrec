package com.sqlrec.model.tzrec;

import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelTrainConf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WideAndDeepModelTest {

    @Test
    public void testGenModelTrainK8sYaml() {
        WideAndDeepModel modelController = new WideAndDeepModel();

        ModelConfig model = new ModelConfig();
        model.modelName = "test-model";

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.name = "test-train-conf";
        trainConf.params = new java.util.HashMap<>();

        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        System.out.println(k8sYaml);

        assertNotNull(k8sYaml);
    }
}
