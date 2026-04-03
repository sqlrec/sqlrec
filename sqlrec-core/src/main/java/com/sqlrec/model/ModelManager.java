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
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

        ModelConfig modelConfig = getAndCheckModel(sqlCreateModel);
        if (HadoopUtils.pathExists(modelConfig.path)) {
            throw new IllegalArgumentException("Model path already exists: " + modelConfig.path);
        }

        saveModel(modelConfig);
        return modelConfig;
    }

    public static void saveModel(ModelConfig modelConfig) {
        Model model = new Model();
        model.setName(modelConfig.modelName);
        model.setDdl(modelConfig.ddl);
        model.setCreatedAt(System.currentTimeMillis());
        model.setUpdatedAt(System.currentTimeMillis());
        DbUtils.insertModel(model);
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
            } else {
                log.info("Model {} re train checkpoint {}, deleting old first",
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

        List<CheckpointInfo> createdCheckpointInfos = new ArrayList<>();
        List<Checkpoint> existingCheckpoints = new ArrayList<>();
        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint existingCheckpoint = DbUtils.getCheckpoint(modelExportConf.modelName, exportCheckpointName);
            if (existingCheckpoint != null) {
                existingCheckpoints.add(existingCheckpoint);
                if (Consts.CHECKPOINT_STATUS_CREATED.equals(existingCheckpoint.getStatus())) {
                    log.info("Model {} has export checkpoint {} in progress",
                            modelExportConf.modelName, exportCheckpointName);
                    createdCheckpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                }
            }
        }

        if (!createdCheckpointInfos.isEmpty()) {
            log.info("Model {} has {} export checkpoints in progress, returning existing checkpoint infos",
                    modelExportConf.modelName, createdCheckpointInfos.size());
            return createdCheckpointInfos;
        }

        for (Checkpoint existingCheckpoint : existingCheckpoints) {
            log.info("Model {} re export checkpoint {}, deleting old first",
                    modelExportConf.modelName, existingCheckpoint.getCheckpointName());
            deleteCheckpoint(modelExportConf.modelName, existingCheckpoint.getCheckpointName());
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

        Map<String, String> userEnvVars = parseEnvVars(params);
        envVars.putAll(userEnvVars);

        if (!envVars.isEmpty()) {
            k8sYaml = K8sManager.injectEnvVarsIntoYaml(k8sYaml, envVars);
        }

        String pvcName = ModelConfigs.CLIENT_PVC_NAME.getValue();
        String pvName = ModelConfigs.CLIENT_PV_NAME.getValue();
        String clientDirValue = ModelConfigs.CLIENT_DIR.getValue();
        k8sYaml = K8sManager.injectVolumeMountIntoYaml(k8sYaml, pvcName, pvName, clientDirValue, null);

        Map<String, String> nodeSelectors = parseNodeSelectors(params);
        if (!nodeSelectors.isEmpty()) {
            k8sYaml = K8sManager.injectNodeSelectorIntoYaml(k8sYaml, nodeSelectors);
        }

        return k8sYaml;
    }

    private static Map<String, String> parseNodeSelectors(Map<String, String> params) {
        Map<String, String> nodeSelectors = new HashMap<>();
        String prefix = "kubernetes.node.selector.";
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String labelKey = entry.getKey().substring(prefix.length());
                nodeSelectors.put(labelKey, entry.getValue());
            }
        }
        return nodeSelectors;
    }

    private static Map<String, String> parseEnvVars(Map<String, String> params) {
        Map<String, String> envVars = new HashMap<>();
        String prefix = "kubernetes.env.";
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String envKey = entry.getKey().substring(prefix.length());
                envVars.put(envKey, entry.getValue());
            }
        }
        return envVars;
    }

    public static void deleteCheckpoint(String modelName, String checkpointName) throws Exception {
        Checkpoint checkpoint = DbUtils.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            return;
        }

        List<Service> services = DbUtils.getServiceListByCheckpoint(modelName, checkpointName);
        if (!services.isEmpty()) {
            throw new IllegalArgumentException("Cannot delete checkpoint " + checkpointName + " for model " + modelName +
                    " because it is being used by " + services.size() + " service(s): " +
                    String.join(", ", services.stream().map(Service::getName).toList()));
        }

        String status = checkpoint.getStatus();
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)) {
            String k8sYaml = checkpoint.getYaml();
            if (!StringUtils.isEmpty(k8sYaml)) {
                K8sManager.deleteYaml(k8sYaml);
            }
        }

        String checkpointPath = ModelEntityConverter.getModelCheckpointPath(checkpoint);
        HadoopUtils.deletePath(checkpointPath);

        DbUtils.deleteCheckpoint(modelName, checkpointName);
    }

    public static void deleteModel(String modelName) throws Exception {
        Model model = DbUtils.getModel(modelName);
        if (model == null) {
            throw new IllegalArgumentException("model not exists: " + modelName);
        }

        List<Service> services = DbUtils.getServiceListByModelName(modelName);
        if (!services.isEmpty()) {
            throw new IllegalArgumentException("Cannot delete model " + modelName +
                    " because it is being used by " + services.size() + " service(s): " +
                    String.join(", ", services.stream().map(Service::getName).toList()));
        }

        List<Checkpoint> checkpoints = DbUtils.getCheckpointListByModelName(modelName);
        for (Checkpoint checkpoint : checkpoints) {
            deleteCheckpoint(modelName, checkpoint.getCheckpointName());
        }

        ModelConfig modelConfig = ModelEntityConverter.convertToModel(model.getDdl());
        HadoopUtils.deletePath(modelConfig.path);

        DbUtils.deleteModel(modelName);
    }

    public static boolean isCheckpointOperationCompleted(List<CheckpointInfo> checkpointInfos) {
        if (checkpointInfos == null || checkpointInfos.isEmpty()) {
            return true;
        }

        boolean allCompleted = true;
        Set<String> yamlsToDelete = new HashSet<>();
        List<String> failedCheckpoints = new ArrayList<>();

        for (CheckpointInfo info : checkpointInfos) {
            Checkpoint checkpoint = DbUtils.getCheckpoint(info.getModelName(), info.getCheckpointName());
            if (checkpoint == null) {
                throw new IllegalArgumentException("Checkpoint not found: " + info.getCheckpointName() + " for model " + info.getModelName());
            }

            String status = checkpoint.getStatus();
            if (Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)) {
                continue;
            }
            if (Consts.CHECKPOINT_STATUS_FAILED.equals(status)) {
                failedCheckpoints.add(info.getCheckpointName() + " for model " + info.getModelName());
                continue;
            }

            if (Consts.CHECKPOINT_STATUS_CREATED.equals(status)) {
                String k8sYaml = checkpoint.getYaml();
                if (StringUtils.isEmpty(k8sYaml)) {
                    checkpoint.setStatus(Consts.CHECKPOINT_STATUS_FAILED);
                    checkpoint.setUpdatedAt(System.currentTimeMillis());
                    DbUtils.upsertCheckpoint(checkpoint);
                    failedCheckpoints.add(info.getCheckpointName() + " for model " + info.getModelName() + " (k8sYaml is empty)");
                    continue;
                }

                String jobStatus = K8sManager.checkJobStatus(k8sYaml);
                if ("succeeded".equals(jobStatus)) {
                    checkpoint.setStatus(Consts.CHECKPOINT_STATUS_SUCCEEDED);
                    checkpoint.setUpdatedAt(System.currentTimeMillis());
                    DbUtils.upsertCheckpoint(checkpoint);
                    yamlsToDelete.add(k8sYaml);
                } else if ("failed".equals(jobStatus)) {
                    checkpoint.setStatus(Consts.CHECKPOINT_STATUS_FAILED);
                    checkpoint.setUpdatedAt(System.currentTimeMillis());
                    DbUtils.upsertCheckpoint(checkpoint);
                    failedCheckpoints.add(info.getCheckpointName() + " for model " + info.getModelName());
                } else {
                    allCompleted = false;
                }
            }
        }

        for (String yaml : yamlsToDelete) {
            if (!StringUtils.isEmpty(yaml)) {
                K8sManager.deleteYaml(yaml);
            }
        }

        if (!failedCheckpoints.isEmpty()) {
            throw new RuntimeException("Checkpoints failed: " + String.join(", ", failedCheckpoints));
        }

        return allCompleted;
    }
}
