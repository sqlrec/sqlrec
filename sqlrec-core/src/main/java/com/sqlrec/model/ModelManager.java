package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelController;
import com.sqlrec.model.common.ModelExportConf;
import com.sqlrec.model.common.ModelTrainConf;
import com.sqlrec.model.common.ServiceConfig;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.DbUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
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
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, modelTrainConf.params);
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

    public static void exportModel(SqlExportModel sqlExportModel, String defaultSchema) throws Exception {
        ModelExportConf modelExportConf = ModelEntityConverter.convertToModelExportConf(sqlExportModel, defaultSchema);

        Model modelEntity = DbUtils.getModel(modelExportConf.modelName);
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + modelExportConf.modelName);
        }

        Checkpoint sourceCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, modelExportConf.checkpointName);
        if (sourceCheckpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + modelExportConf.checkpointName + " for model " + modelExportConf.modelName);
        }

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

        List<String> exportCheckpointNames = modelController.getExportCheckpoints(modelExportConf);
        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint existingCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, exportCheckpointName);
            if (existingCheckpoint != null) {
                throw new IllegalArgumentException("checkpoint already exists: " + exportCheckpointName + " for model " + modelExportConf.modelName);
            }
        }

        String k8sYaml = modelController.genModelExportK8sYaml(modelConfig, modelExportConf);
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, modelExportConf.params);
        K8sManager.applyYaml(k8sYaml);

        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName(modelExportConf.modelName);
            checkpoint.setCheckpointName(exportCheckpointName);
            checkpoint.setYaml(k8sYaml);
            checkpoint.setDdl(CompileManager.getSqlStr(sqlExportModel));
            checkpoint.setCheckpointType(Consts.CHECKPOINT_TYPE_EXPORT);
            checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
            checkpoint.setCreatedAt(System.currentTimeMillis());
            checkpoint.setUpdatedAt(System.currentTimeMillis());

            DbUtils.upsertCheckpoint(checkpoint);
        }
    }

    public static String injectPodConfig(String k8sYaml, ModelConfig model, Map<String, String> params) {
        String namespace;
        if (params.containsKey(ModelConfigs.NAMESPACE.getKey())) {
            namespace = params.get(ModelConfigs.NAMESPACE.getKey());
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

    public static Service createService(SqlCreateService sqlCreateService) throws Exception {
        ServiceConfig serviceConfig = ModelEntityConverter.convertToServiceConf(sqlCreateService);

        Model modelEntity = DbUtils.getModel(serviceConfig.modelName);
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + serviceConfig.modelName);
        }

        Checkpoint checkpoint = DbUtils.getCheckpoint(serviceConfig.modelName, serviceConfig.checkpointName);
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + serviceConfig.checkpointName + " for model " + serviceConfig.modelName);
        }

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

        String serviceUrl = modelController.getServiceUrl(modelConfig, serviceConfig);
        String k8sYaml = modelController.getServiceK8sYaml(modelConfig, serviceConfig);
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, serviceConfig.params);
        K8sManager.applyYaml(k8sYaml);

        Service service = new Service();
        service.setName(serviceConfig.serviceName);
        service.setModelName(serviceConfig.modelName);
        service.setCheckpointName(serviceConfig.checkpointName);
        service.setDdl(CompileManager.getSqlStr(sqlCreateService));
        service.setYaml(k8sYaml);
        service.setUrl(serviceUrl);
        service.setCreatedAt(System.currentTimeMillis());
        service.setUpdatedAt(System.currentTimeMillis());
        service.setIfNotExists(sqlCreateService.isIfNotExists());

        if (sqlCreateService.isIfNotExists()) {
            DbUtils.insertService(service);
        } else {
            DbUtils.upsertService(service);
        }

        return service;
    }

    public static void deleteService(String serviceName) {
        Service service = DbUtils.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("service not exists: " + serviceName);
        }
        if (!StringUtils.isEmpty(service.getYaml())) {
            K8sManager.deleteYaml(service.getYaml());
        }
        DbUtils.deleteService(serviceName);
    }
}
