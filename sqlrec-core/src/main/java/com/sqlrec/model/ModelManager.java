package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelManager {
    private static final Logger log = LoggerFactory.getLogger(ModelManager.class);

    public static ModelConfig getAndCheckModel(SqlCreateModel sqlCreateModel) {
        try {
            ModelConfig model = ModelEntityConverter.convertToModel(sqlCreateModel);
            ModelController modelController = ModelControllerFactory.getModelController(model);
            if (modelController == null) {
                throw new IllegalArgumentException("Model controller not found for model name: " + model.modelName);
            }
            String errorMessage = modelController.checkModel(model);
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }

            List<FieldSchema> inputFields = model.inputFields;
            List<FieldSchema> outputFields = modelController.getOutputFields(model);

            if (inputFields != null && outputFields != null) {
                for (FieldSchema inputField : inputFields) {
                    for (FieldSchema outputField : outputFields) {
                        if (inputField.name.equalsIgnoreCase(outputField.name)) {
                            throw new IllegalArgumentException("Field '" + inputField.name + "' exists in both input fields and output fields");
                        }
                    }
                }
            }

            return model;
        } catch (Exception e) {
            throw new RuntimeException("Error while checking model: " + e.getMessage(), e);
        }
    }

    public static ModelConfig createModel(SqlCreateModel sqlCreateModel) {
        String modelName = sqlCreateModel.getModelName().getSimple();
        boolean ifNotExists = sqlCreateModel.isIfNotExists();

        Model existingModel = DbUtils.getModel(modelName);
        if (existingModel != null) {
            if (ifNotExists) {
                return null;
            }
            throw new IllegalArgumentException("Model already exists: " + modelName);
        }

        String modelPath = ModelEntityConverter.getModelPath(modelName);
        if (HadoopUtils.pathExists(modelPath)) {
            throw new IllegalArgumentException("Model path already exists: " + modelPath);
        }

        ModelConfig modelConfig = getAndCheckModel(sqlCreateModel);
        saveModel(sqlCreateModel);
        return modelConfig;
    }

    public static void saveModel(SqlCreateModel sqlCreateModel) {
        Model model = new Model();
        model.setName(sqlCreateModel.getModelName().getSimple());
        model.setDdl(sqlCreateModel.toString());
        model.setCreatedAt(System.currentTimeMillis());
        model.setUpdatedAt(System.currentTimeMillis());
        if (sqlCreateModel.isIfNotExists()) {
            DbUtils.insertModel(model);
        } else {
            DbUtils.upsertModel(model);
        }
    }

    public static List<CheckpointInfo> trainModel(SqlTrainModel sqlTrainModel, String defaultSchema) throws Exception {
        ModelTrainConf modelTrainConf = ModelEntityConverter.convertToModelTrainConf(sqlTrainModel, defaultSchema);

        Model modelEntity = DbUtils.getModel(modelTrainConf.modelName);
        ModelConfig modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        ModelController modelController = ModelControllerFactory.getModelController(modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + modelConfig.modelName);
        }

        Checkpoint existingCheckpoint = DbUtils.getCheckpoint(modelTrainConf.modelName, modelTrainConf.checkpointName);
        if (existingCheckpoint != null) {
            String status = existingCheckpoint.getStatus();
            if (Consts.CHECKPOINT_STATUS_CREATED.equals(status)) {
                log.info("Model {} has checkpoint {} in progress, returning existing checkpoint info",
                        modelTrainConf.modelName, existingCheckpoint.getCheckpointName());
                List<CheckpointInfo> checkpointInfos = new ArrayList<>();
                checkpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                return checkpointInfos;
            }
            if (Consts.CHECKPOINT_STATUS_FAILED.equals(status)) {
                log.info("Model {} has failed checkpoint {}, deleting it first",
                        modelTrainConf.modelName, existingCheckpoint.getCheckpointName());
                deleteCheckpoint(modelTrainConf.modelName, existingCheckpoint.getCheckpointName());
            }
        }

        String k8sYaml = modelController.genModelTrainK8sYaml(modelConfig, modelTrainConf);
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, modelTrainConf.params);
        K8sManager.applyYaml(k8sYaml);

        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName(modelTrainConf.modelName);
        checkpoint.setCheckpointName(modelTrainConf.checkpointName);
        checkpoint.setModelDdl(modelEntity.getDdl());
        checkpoint.setYaml(k8sYaml);
        checkpoint.setDdl(CompileManager.getSqlStr(sqlTrainModel));
        checkpoint.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setCreatedAt(System.currentTimeMillis());
        checkpoint.setUpdatedAt(System.currentTimeMillis());

        DbUtils.upsertCheckpoint(checkpoint);

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        checkpointInfos.add(new CheckpointInfo(modelTrainConf.modelName, modelTrainConf.checkpointName));
        return checkpointInfos;
    }

    public static List<CheckpointInfo> exportModel(SqlExportModel sqlExportModel, String defaultSchema) throws Exception {
        ModelExportConf modelExportConf = ModelEntityConverter.convertToModelExportConf(sqlExportModel, defaultSchema);

        Model modelEntity = DbUtils.getModel(modelExportConf.modelName);
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + modelExportConf.modelName);
        }

        Checkpoint sourceCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, modelExportConf.checkpointName);
        if (sourceCheckpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + modelExportConf.checkpointName + " for model " + modelExportConf.modelName);
        }

        ModelConfig modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        ModelController modelController = ModelControllerFactory.getModelController(modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + modelConfig.modelName);
        }

        List<String> exportCheckpointNames = modelController.getExportCheckpoints(modelExportConf);

        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint existingCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, exportCheckpointName);
            if (existingCheckpoint != null) {
                String status = existingCheckpoint.getStatus();
                if (Consts.CHECKPOINT_STATUS_CREATED.equals(status)) {
                    log.info("Model {} has export checkpoint {} in progress, returning existing checkpoint info",
                            modelExportConf.modelName, exportCheckpointName);
                    List<CheckpointInfo> checkpointInfos = new ArrayList<>();
                    checkpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                    return checkpointInfos;
                }
                if (Consts.CHECKPOINT_STATUS_FAILED.equals(status)) {
                    log.info("Model {} has failed export checkpoint {}, deleting it first",
                            modelExportConf.modelName, exportCheckpointName);
                    deleteCheckpoint(modelExportConf.modelName, exportCheckpointName);
                }
            }
        }

        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint existingCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, exportCheckpointName);
            if (existingCheckpoint != null) {
                throw new IllegalArgumentException("checkpoint already exists: " + exportCheckpointName + " for model " + modelExportConf.modelName);
            }
        }

        String k8sYaml = modelController.genModelExportK8sYaml(modelConfig, modelExportConf);
        k8sYaml = injectPodConfig(k8sYaml, modelConfig, modelExportConf.params);
        K8sManager.applyYaml(k8sYaml);

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName(modelExportConf.modelName);
            checkpoint.setCheckpointName(exportCheckpointName);
            checkpoint.setModelDdl(modelEntity.getDdl());
            checkpoint.setYaml(k8sYaml);
            checkpoint.setDdl(CompileManager.getSqlStr(sqlExportModel));
            checkpoint.setCheckpointType(Consts.CHECKPOINT_TYPE_EXPORT);
            checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
            checkpoint.setCreatedAt(System.currentTimeMillis());
            checkpoint.setUpdatedAt(System.currentTimeMillis());

            DbUtils.upsertCheckpoint(checkpoint);
            checkpointInfos.add(new CheckpointInfo(modelExportConf.modelName, exportCheckpointName));
        }

        return checkpointInfos;
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

    public static String createService(SqlCreateService sqlCreateService) throws Exception {
        ServiceConfig serviceConfig = ModelEntityConverter.convertToServiceConf(sqlCreateService);

        Model modelEntity = DbUtils.getModel(serviceConfig.modelName);
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + serviceConfig.modelName);
        }

        Checkpoint checkpoint = DbUtils.getCheckpoint(serviceConfig.modelName, serviceConfig.checkpointName);
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + serviceConfig.checkpointName + " for model " + serviceConfig.modelName);
        }
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(checkpoint.getStatus())) {
            throw new IllegalArgumentException("checkpoint status is not succeeded: " + checkpoint.getStatus() + " for checkpoint " + serviceConfig.checkpointName + " for model " + serviceConfig.modelName);
        }
        if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
            throw new IllegalArgumentException("service only supports export checkpoint");
        }

        ModelController modelController = ModelControllerFactory.getModelController(serviceConfig.modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + serviceConfig.modelConfig.modelName);
        }

        String serviceUrl = modelController.getServiceUrl(serviceConfig.modelConfig, serviceConfig);
        String k8sYaml = modelController.getServiceK8sYaml(serviceConfig.modelConfig, serviceConfig);
        if (!StringUtils.isEmpty(k8sYaml)) {
            k8sYaml = injectPodConfig(k8sYaml, serviceConfig.modelConfig, serviceConfig.params);
            K8sManager.applyYaml(k8sYaml);
        }

        Service service = new Service();
        service.setName(serviceConfig.serviceName);
        service.setModelName(serviceConfig.modelName);
        service.setModelDdl(checkpoint.getModelDdl());
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

        return serviceConfig.serviceName;
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

    public static void deleteCheckpoint(String modelName, String checkpointName) {
        Checkpoint checkpoint = DbUtils.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            return;
        }

        String status = checkpoint.getStatus();
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)) {
            String k8sYaml = checkpoint.getYaml();
            if (!StringUtils.isEmpty(k8sYaml)) {
                K8sManager.deleteYaml(k8sYaml);
            }
        }

        String checkpointPath = ModelEntityConverter.getModelCheckpointPath(modelName, checkpointName);
        HadoopUtils.deletePath(checkpointPath);

        DbUtils.deleteCheckpoint(modelName, checkpointName);
    }

    public static void deleteModel(String modelName) {
        List<Checkpoint> checkpoints = DbUtils.getCheckpointListByModelName(modelName);
        for (Checkpoint checkpoint : checkpoints) {
            deleteCheckpoint(modelName, checkpoint.getCheckpointName());
        }

        String modelPath = ModelEntityConverter.getModelPath(modelName);
        HadoopUtils.deletePath(modelPath);

        DbUtils.deleteModel(modelName);
    }

    public static boolean isCheckpointOperationCompleted(String modelName, String checkpointName) {
        Checkpoint checkpoint = DbUtils.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            throw new IllegalArgumentException("Checkpoint not found: " + checkpointName + " for model " + modelName);
        }

        String status = checkpoint.getStatus();

        if (Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)) {
            return true;
        }
        if (Consts.CHECKPOINT_STATUS_FAILED.equals(status)) {
            throw new RuntimeException("Checkpoint " + checkpointName + " for model " + modelName + " failed");
        }

        if (Consts.CHECKPOINT_STATUS_CREATED.equals(status)) {
            String k8sYaml = checkpoint.getYaml();
            if (StringUtils.isEmpty(k8sYaml)) {
                return false;
            }

            String jobStatus = K8sManager.checkJobStatus(k8sYaml);
            if ("succeeded".equals(jobStatus)) {
                checkpoint.setStatus(Consts.CHECKPOINT_STATUS_SUCCEEDED);
                checkpoint.setUpdatedAt(System.currentTimeMillis());
                DbUtils.upsertCheckpoint(checkpoint);

                if (!StringUtils.isEmpty(k8sYaml)) {
                    K8sManager.deleteYaml(k8sYaml);
                }

                return true;
            }
            if ("failed".equals(jobStatus)) {
                checkpoint.setStatus(Consts.CHECKPOINT_STATUS_FAILED);
                checkpoint.setUpdatedAt(System.currentTimeMillis());
                DbUtils.upsertCheckpoint(checkpoint);
                throw new RuntimeException("Checkpoint " + checkpointName + " for model " + modelName + " failed");
            }
        }

        return false;
    }
}
