package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelController;
import com.sqlrec.model.common.ModelTrainConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.DbUtils;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ModelManager {
    private static final Logger log = LoggerFactory.getLogger(ModelManager.class);

    public static ModelConfig getAndCheckModel(SqlCreateModel sqlCreateModel) {
        try {
            ModelConfig model = ModelEntityConverter.convertToModel(sqlCreateModel);
            String modelName = ModelConfigs.MODEL.getValue(model.params);
            ModelController modelController = ModelControllerFactory.getModelController(modelName);
            if (modelController == null) {
                throw new IllegalArgumentException("Model controller not found for model name: " + modelName);
            }
            String errorMessage = modelController.checkModel(model);
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }
            return model;
        } catch (Exception e) {
            throw new RuntimeException("Error while checking model: " + e.getMessage(), e);
        }
    }

    public static Checkpoint trainModel(SqlTrainModel sqlTrainModel, String defaultSchema) throws Exception {
        ModelTrainConf modelTrainConf = ModelEntityConverter.convertToModelTrainConf(sqlTrainModel, defaultSchema);

        Model modelEntity = DbUtils.getModel(modelTrainConf.modelName);
        SqlNode modelSqlNode = CompileManager.parseFlinkSql(modelEntity.getDdl());
        if (!(modelSqlNode instanceof SqlCreateModel)) {
            throw new IllegalArgumentException("Invalid model DDL: " + modelEntity.getDdl());
        }
        ModelConfig modelConfig = ModelEntityConverter.convertToModel((SqlCreateModel) modelSqlNode);

        String modelAlgorithmName = ModelConfigs.MODEL.getValue(modelConfig.params);
        ModelController modelController = ModelControllerFactory.getModelController(modelAlgorithmName);
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + modelAlgorithmName);
        }

        String k8sYaml = modelController.genModelTrainK8sYaml(modelConfig, modelTrainConf);
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, modelTrainConf);
        K8sManager.applyYaml(k8sYaml);

        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName(modelTrainConf.modelName);
        checkpoint.setCheckpointName(modelTrainConf.checkpointName);
        checkpoint.setYaml(k8sYaml);
        checkpoint.setDdl(CompileManager.getSqlStr(sqlTrainModel));
        checkpoint.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setCreatedAt(System.currentTimeMillis());
        checkpoint.setUpdatedAt(System.currentTimeMillis());

        DbUtils.upsertCheckpoint(checkpoint);
        return checkpoint;
    }

    public static String injectPodConfig(String k8sYaml, ModelConfig model, ModelTrainConf modelTrainConf) {
        String namespace;
        if (modelTrainConf.params.containsKey(ModelConfigs.NAMESPACE.getKey())) {
            namespace = modelTrainConf.params.get(ModelConfigs.NAMESPACE.getKey());
        } else {
            namespace = ModelConfigs.NAMESPACE.getValue();
        }

        k8sYaml = K8sManager.injectNamespaceIntoYaml(k8sYaml, namespace);

        Map<String, String> envVars = new HashMap<>();

        String javaHome = ModelConfigs.JAVA_HOME.getValue();
        if (javaHome != null) {
            envVars.put("JAVA_HOME", javaHome);
        }
        String hadoopHome = ModelConfigs.HADOOP_HOME.getValue();
        if (hadoopHome != null) {
            envVars.put("HADOOP_HOME", hadoopHome);
        }
        String classpath = ModelConfigs.CLASSPATH.getValue();
        if (classpath != null) {
            envVars.put("CLASSPATH", classpath);
        }
        String hadoopConfDir = ModelConfigs.HADOOP_CONF_DIR.getValue();
        if (hadoopConfDir != null) {
            envVars.put("HADOOP_CONF_DIR", hadoopConfDir);
        }
        String clientDir = ModelConfigs.CLIENT_DIR.getValue();
        if (clientDir != null) {
            envVars.put("CLIENT_DIR", clientDir);
        }

        if (!envVars.isEmpty()) {
            k8sYaml = K8sManager.injectEnvVarsIntoYaml(k8sYaml, envVars);
        }

        String pvcName = ModelConfigs.CLIENT_PVC_NAME.getValue();
        String pvName = ModelConfigs.CLIENT_PV_NAME.getValue();
        String clientDirValue = ModelConfigs.CLIENT_DIR.getValue();
        k8sYaml = K8sManager.injectVolumeMountIntoYaml(k8sYaml, pvcName, pvName, clientDirValue, null);

        return k8sYaml;
    }
}
