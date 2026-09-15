package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.utils.PathUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ModelManager {
    private static final Logger log = LoggerFactory.getLogger(ModelManager.class);

    public static ModelConf getAndCheckModel(SqlCreateModel sqlCreateModel) {
        try {
            ModelConf model = ModelEntityConverter.convertToModel(sqlCreateModel);
            ModelController modelController = ModelControllerFactory.getRequiredModelController(model);
            String errorMessage = modelController.checkModel(model);
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }

            checkInputAndOutputFieldNames(
                    model.getInputFields(),
                    modelController.getOutputFields(model)
            );

            return model;
        } catch (Exception e) {
            throw new RuntimeException("Error while checking model: " + e.getMessage(), e);
        }
    }

    private static void checkInputAndOutputFieldNames(
            List<FieldSchema> inputFields,
            List<FieldSchema> outputFields
    ) {
        if (inputFields == null || outputFields == null) {
            return;
        }
        for (FieldSchema inputField : inputFields) {
            for (FieldSchema outputField : outputFields) {
                if (inputField.getName().equalsIgnoreCase(outputField.getName())) {
                    throw new IllegalArgumentException("Field '" + inputField.getName()
                            + "' exists in both input fields and output fields");
                }
            }
        }
    }

    public static ModelConf createModel(SqlCreateModel sqlCreateModel) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        String modelName = ResourceNames.of(sqlCreateModel.getModelName());
        Model existingModel = db.getModel(modelName);
        if (existingModel != null) {
            if (sqlCreateModel.isIfNotExists()) {
                return null;
            }
            throw new IllegalArgumentException("Model already exists: " + modelName);
        }

        ModelConf modelConfig = getAndCheckModel(sqlCreateModel);
        if (db.hdfsPathExists(modelConfig.getPath())) {
            throw new IllegalArgumentException("Model path already exists: " + modelConfig.getPath());
        }

        saveModel(modelConfig);
        return modelConfig;
    }

    public static void saveModel(ModelConf modelConfig) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Model model = new Model();
        model.setName(modelConfig.getModelName());
        model.setDdl(modelConfig.getDdl());
        model.setCreatedAt(System.currentTimeMillis());
        model.setUpdatedAt(System.currentTimeMillis());
        db.insertModel(model);
    }

    public static List<CheckpointInfo> trainModel(SqlTrainModel sqlTrainModel, String defaultSchema) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        ModelTrainConf modelTrainConf = ModelEntityConverter.convertToModelTrainConf(sqlTrainModel, defaultSchema);

        Model modelEntity = db.getModel(modelTrainConf.getModelName());
        ModelConf modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        ModelController modelController = ModelControllerFactory.getRequiredModelController(modelConfig);

        Checkpoint existingCheckpoint = db.getCheckpoint(modelTrainConf.getModelName(), modelTrainConf.getCheckpointName());
        if (existingCheckpoint != null) {
            String status = existingCheckpoint.getStatus();
            if (Consts.CHECKPOINT_STATUS_CREATED.equals(status)) {
                log.info("Model {} has checkpoint {} in progress, returning existing checkpoint info",
                        modelTrainConf.getModelName(), existingCheckpoint.getCheckpointName());
                List<CheckpointInfo> checkpointInfos = new ArrayList<>();
                checkpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                return checkpointInfos;
            } else {
                log.info("Model {} re train checkpoint {}, deleting old first",
                        modelTrainConf.getModelName(), existingCheckpoint.getCheckpointName());
                deleteCheckpoint(modelTrainConf.getModelName(), existingCheckpoint.getCheckpointName());
            }
        }

        if (modelController.requiresTrainingData()
                && (modelTrainConf.getTrainDataPaths() == null || modelTrainConf.getTrainDataPaths().isEmpty())) {
            throw new IllegalArgumentException("TRAIN MODEL ON data source is required for model type: "
                    + ModelConfigs.MODEL.getValue(modelConfig.getParams()));
        }

        String k8sYaml = modelController.genModelTrainK8sYaml(modelConfig, modelTrainConf);
        k8sYaml = K8sYamlUtils.injectPodConfig(k8sYaml, modelConfig, modelTrainConf.getParams());

        Checkpoint checkpoint = createCheckpoint(
                modelTrainConf.getModelName(),
                modelTrainConf.getCheckpointName(),
                modelEntity.getDdl(),
                k8sYaml,
                CompileManager.getSqlStr(sqlTrainModel),
                Consts.CHECKPOINT_TYPE_ORIGIN
        );

        db.insertCheckpoint(checkpoint);
        K8sManager.applyYaml(k8sYaml);

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        checkpointInfos.add(new CheckpointInfo(modelTrainConf.getModelName(), modelTrainConf.getCheckpointName()));
        return checkpointInfos;
    }

    public static List<CheckpointInfo> exportModel(SqlExportModel sqlExportModel, String defaultSchema) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        ModelExportConf modelExportConf = ModelEntityConverter.convertToModelExportConf(sqlExportModel, defaultSchema);

        Model modelEntity = db.getModel(modelExportConf.getModelName());
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + modelExportConf.getModelName());
        }

        Checkpoint sourceCheckpoint = db.getCheckpoint(modelExportConf.getModelName(), modelExportConf.getCheckpointName());
        if (sourceCheckpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + modelExportConf.getCheckpointName() + " for model " + modelExportConf.getModelName());
        }

        ModelConf modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        ModelController modelController = ModelControllerFactory.getRequiredModelController(modelConfig);

        List<String> exportCheckpointNames = modelController.getExportCheckpoints(modelExportConf);

        List<CheckpointInfo> createdCheckpointInfos = new ArrayList<>();
        List<Checkpoint> existingCheckpoints = new ArrayList<>();
        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint existingCheckpoint = db.getCheckpoint(modelExportConf.getModelName(), exportCheckpointName);
            if (existingCheckpoint != null) {
                existingCheckpoints.add(existingCheckpoint);
                if (Consts.CHECKPOINT_STATUS_CREATED.equals(existingCheckpoint.getStatus())) {
                    log.info("Model {} has export checkpoint {} in progress",
                            modelExportConf.getModelName(), exportCheckpointName);
                    createdCheckpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                }
            }
        }

        if (!createdCheckpointInfos.isEmpty()) {
            log.info("Model {} has {} export checkpoints in progress, returning existing checkpoint infos",
                    modelExportConf.getModelName(), createdCheckpointInfos.size());
            return createdCheckpointInfos;
        }

        for (Checkpoint existingCheckpoint : existingCheckpoints) {
            log.info("Model {} re export checkpoint {}, deleting old first",
                    modelExportConf.getModelName(), existingCheckpoint.getCheckpointName());
            deleteCheckpoint(modelExportConf.getModelName(), existingCheckpoint.getCheckpointName());
        }

        String exportCleanPath = modelController.getExportCleanPath(modelExportConf);
        if (StringUtils.isNotEmpty(exportCleanPath)) {
            PathUtils.validateModelPath(exportCleanPath, modelConfig.getPath());
            db.hdfsDeletePath(exportCleanPath);
        }

        String k8sYaml = modelController.genModelExportK8sYaml(modelConfig, modelExportConf);
        k8sYaml = K8sYamlUtils.injectPodConfig(k8sYaml, modelConfig, modelExportConf.getParams());

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        for (String exportCheckpointName : exportCheckpointNames) {
            Checkpoint checkpoint = createCheckpoint(
                    modelExportConf.getModelName(),
                    exportCheckpointName,
                    modelEntity.getDdl(),
                    k8sYaml,
                    CompileManager.getSqlStr(sqlExportModel),
                    Consts.CHECKPOINT_TYPE_EXPORT
            );

            db.insertCheckpoint(checkpoint);
            checkpointInfos.add(new CheckpointInfo(modelExportConf.getModelName(), exportCheckpointName));
        }

        K8sManager.applyYaml(k8sYaml);

        return checkpointInfos;
    }

    private static Checkpoint createCheckpoint(
            String modelName,
            String checkpointName,
            String modelDdl,
            String k8sYaml,
            String checkpointDdl,
            String checkpointType
    ) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName(modelName);
        checkpoint.setCheckpointName(checkpointName);
        checkpoint.setModelDdl(modelDdl);
        checkpoint.setYaml(k8sYaml);
        checkpoint.setDdl(checkpointDdl);
        checkpoint.setCheckpointType(checkpointType);
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setCreatedAt(System.currentTimeMillis());
        checkpoint.setUpdatedAt(System.currentTimeMillis());
        return checkpoint;
    }

    public static void deleteCheckpoint(String modelName, String checkpointName) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Checkpoint checkpoint = db.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            return;
        }

        List<Service> services = db.getServiceListByCheckpoint(modelName, checkpointName);
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

        ModelConf modelConfig = ModelEntityConverter.convertToModel(checkpoint.getModelDdl());
        String checkpointPath = ModelEntityConverter.getModelCheckpointPath(checkpoint);
        PathUtils.validateModelPath(checkpointPath, modelConfig.getPath());
        db.hdfsDeletePath(checkpointPath);

        db.deleteCheckpoint(modelName, checkpointName);
    }

    public static void deleteModel(String modelName) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Model model = db.getModel(modelName);
        if (model == null) {
            throw new IllegalArgumentException("model not exists: " + modelName);
        }

        List<Service> services = db.getServiceListByModelName(modelName);
        if (!services.isEmpty()) {
            throw new IllegalArgumentException("Cannot delete model " + modelName +
                    " because it is being used by " + services.size() + " service(s): " +
                    String.join(", ", services.stream().map(Service::getName).toList()));
        }

        List<Checkpoint> checkpoints = db.getCheckpointListByModelName(modelName);
        for (Checkpoint checkpoint : checkpoints) {
            deleteCheckpoint(modelName, checkpoint.getCheckpointName());
        }

        ModelConf modelConfig = ModelEntityConverter.convertToModel(model.getDdl());
        db.hdfsDeletePath(modelConfig.getPath());

        db.deleteModel(modelName);
    }

    public static boolean isCheckpointOperationCompleted(List<CheckpointInfo> checkpointInfos) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        if (checkpointInfos == null || checkpointInfos.isEmpty()) {
            return true;
        }

        boolean allCompleted = true;
        Set<String> yamlsToDelete = new HashSet<>();
        List<String> failedCheckpoints = new ArrayList<>();

        for (CheckpointInfo info : checkpointInfos) {
            Checkpoint checkpoint = db.getCheckpoint(info.getModelName(), info.getCheckpointName());
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
                    updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_FAILED);
                    failedCheckpoints.add(info.getCheckpointName() + " for model " + info.getModelName() + " (k8sYaml is empty)");
                    continue;
                }

                String jobStatus = K8sManager.checkJobsStatusFromYaml(k8sYaml);
                if ("succeeded".equals(jobStatus)) {
                    updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_SUCCEEDED);
                    yamlsToDelete.add(k8sYaml);
                } else if ("failed".equals(jobStatus)) {
                    updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_FAILED);
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

    private static void updateCheckpointStatus(
            MetadataAccess db,
            Checkpoint checkpoint,
            String status
    ) {
        checkpoint.setStatus(status);
        checkpoint.setUpdatedAt(System.currentTimeMillis());
        db.upsertCheckpoint(checkpoint);
    }
}
