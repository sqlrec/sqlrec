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
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
        ModelTrainConf modelTrainConf = ModelEntityConverter.convertToModelTrainConf(sqlTrainModel);

        Model modelEntity = getRequiredModel(db, modelTrainConf.getModelName());
        ModelConf modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        Checkpoint existingCheckpoint = db.getCheckpoint(modelTrainConf.getModelName(), modelTrainConf.getCheckpointName());
        modelTrainConf.setModelDir(checkpointPath(modelConfig, existingCheckpoint, modelTrainConf.getCheckpointName()));
        if (sqlTrainModel.getExistingCheckpoint() != null) {
            String sourceName = ResourceNames.normalize(
                    SchemaUtils.removeQuotes(sqlTrainModel.getExistingCheckpoint().toString()));
            Checkpoint source = sourceName.equals(modelTrainConf.getCheckpointName())
                    ? existingCheckpoint : db.getCheckpoint(modelTrainConf.getModelName(), sourceName);
            modelTrainConf.setBaseModelDir(checkpointPath(modelConfig, source, sourceName));
        }
        modelTrainConf.setTrainDataPaths(loadTrainingDataPaths(
                db, sqlTrainModel.getDataSource(), sqlTrainModel.getWhereCondition(), defaultSchema));
        PathUtils.validateModelPath(modelTrainConf.getModelDir(), modelConfig.getPath());
        ModelController modelController = ModelControllerFactory.getRequiredModelController(modelConfig);

        if (existingCheckpoint != null) {
            if (Consts.CHECKPOINT_TYPE_EXPORT.equals(existingCheckpoint.getCheckpointType())) {
                throw new IllegalArgumentException("TRAIN MODEL target cannot be an export checkpoint; use a new checkpoint name");
            }
            String status = existingCheckpoint.getStatus();
            if (Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)
                    || (Consts.CHECKPOINT_STATUS_CREATED.equals(status)
                    && CheckpointCompletion.canReuseCreated(db, existingCheckpoint))) {
                log.info("Model {} reuses checkpoint {} with status {}",
                        modelTrainConf.getModelName(), existingCheckpoint.getCheckpointName(), status);
                List<CheckpointInfo> checkpointInfos = new ArrayList<>();
                checkpointInfos.add(new CheckpointInfo(existingCheckpoint.getModelName(), existingCheckpoint.getCheckpointName()));
                return checkpointInfos;
            }
        } else {
            validateNewCheckpointPath(db, modelTrainConf.getModelDir());
        }

        if (modelController.requiresTrainingData()
                && (modelTrainConf.getTrainDataPaths() == null || modelTrainConf.getTrainDataPaths().isEmpty())) {
            throw new IllegalArgumentException("TRAIN MODEL ON data source is required for model type: "
                    + ModelConfigs.MODEL.getValue(modelConfig.getParams()));
        }

        if (StringUtils.isNotEmpty(modelTrainConf.getBaseModelDir())
                && new Path(modelTrainConf.getBaseModelDir()).equals(new Path(modelTrainConf.getModelDir()))) {
            throw new IllegalArgumentException("Training source checkpoint must differ from target checkpoint");
        }
        modelTrainConf.setId(K8sYamlUtils.checkpointAttemptJobId("train",
                modelTrainConf.getModelName(), modelTrainConf.getCheckpointName()));
        String k8sYaml = modelController.genModelTrainK8sYaml(modelConfig, modelTrainConf);
        k8sYaml = K8sYamlUtils.injectPodConfig(k8sYaml, modelConfig, modelTrainConf.getParams());
        K8sManager.validateYaml(k8sYaml);

        if (existingCheckpoint != null) {
            validateCheckpointModification(db, existingCheckpoint);
            if (StringUtils.isNotEmpty(existingCheckpoint.getYaml())) {
                K8sManager.deleteYamlAndWait(existingCheckpoint.getYaml());
            }
        }
        Checkpoint checkpoint = createCheckpoint(
                modelTrainConf.getModelName(),
                modelTrainConf.getCheckpointName(),
                modelEntity.getDdl(),
                k8sYaml,
                CompileManager.getSqlStr(sqlTrainModel),
                Consts.CHECKPOINT_TYPE_ORIGIN
        );

        if (existingCheckpoint == null) {
            db.insertCheckpoint(checkpoint);
        } else {
            checkpoint.setCreatedAt(existingCheckpoint.getCreatedAt());
            checkpoint.setModelDdl(existingCheckpoint.getModelDdl());
            db.upsertCheckpoint(checkpoint);
        }
        submitCheckpoints(db, k8sYaml, List.of(checkpoint));

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        checkpointInfos.add(new CheckpointInfo(modelTrainConf.getModelName(), modelTrainConf.getCheckpointName()));
        return checkpointInfos;
    }

    public static List<CheckpointInfo> exportModel(SqlExportModel sqlExportModel, String defaultSchema) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        ModelExportConf modelExportConf = ModelEntityConverter.convertToModelExportConf(sqlExportModel);

        Model modelEntity = getRequiredModel(db, modelExportConf.getModelName());
        ModelConf modelConfig = ModelEntityConverter.convertToModel(modelEntity.getDdl());
        Checkpoint sourceCheckpoint = db.getCheckpoint(modelExportConf.getModelName(), modelExportConf.getCheckpointName());
        if (sourceCheckpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + modelExportConf.getCheckpointName() + " for model " + modelExportConf.getModelName());
        }
        modelExportConf.setBaseModelDir(checkpointPath(modelConfig, sourceCheckpoint, modelExportConf.getCheckpointName()));
        modelExportConf.setTrainDataPaths(loadTrainingDataPaths(
                db, sqlExportModel.getDataSource(), sqlExportModel.getWhereCondition(), defaultSchema));

        CheckpointCompletion.refresh(db, sourceCheckpoint);
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(sourceCheckpoint.getStatus())) {
            throw new IllegalArgumentException("Export source checkpoint must be succeeded: "
                    + sourceCheckpoint.getCheckpointName());
        }
        if (Consts.CHECKPOINT_TYPE_EXPORT.equals(sourceCheckpoint.getCheckpointType())) {
            throw new IllegalArgumentException("Export source must be an origin checkpoint");
        }
        ModelController modelController = ModelControllerFactory.getRequiredModelController(modelConfig);

        List<String> exportCheckpointNames = modelController.getExportCheckpoints(modelExportConf);
        ExportCheckpoints checkpoints = findExportCheckpoints(
                db, modelExportConf.getModelName(), modelConfig.getPath(), exportCheckpointNames);

        if (!checkpoints.inProgress().isEmpty()) {
            log.info("Model {} has {} export checkpoints in progress, returning existing checkpoint infos",
                    modelExportConf.getModelName(), checkpoints.inProgress().size());
            return checkpoints.inProgress();
        }

        String cleanPath = modelController.getExportCleanPath(modelExportConf);
        if (StringUtils.isNotEmpty(cleanPath)) {
            PathUtils.validateModelPath(cleanPath, modelConfig.getPath());
            if (checkpoints.existing().isEmpty()) {
                validateNewCheckpointPath(db, cleanPath);
            }
        }

        modelExportConf.setId(K8sYamlUtils.checkpointAttemptJobId("export",
                modelExportConf.getModelName(), modelExportConf.getCheckpointName()));
        String k8sYaml = modelController.genModelExportK8sYaml(modelConfig, modelExportConf);
        k8sYaml = K8sYamlUtils.injectPodConfig(k8sYaml, modelConfig, modelExportConf.getParams());
        K8sManager.validateYaml(k8sYaml);

        deleteExportCheckpoints(db, modelExportConf.getModelName(), checkpoints.existing());
        if (StringUtils.isNotEmpty(cleanPath)) db.hdfsDeletePath(cleanPath);

        List<CheckpointInfo> checkpointInfos = new ArrayList<>();
        List<Checkpoint> submitted = new ArrayList<>();
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
            submitted.add(checkpoint);
            checkpointInfos.add(new CheckpointInfo(modelExportConf.getModelName(), exportCheckpointName));
        }

        submitCheckpoints(db, k8sYaml, submitted);

        return checkpointInfos;
    }

    private static void submitCheckpoints(MetadataAccess db, String yaml, List<Checkpoint> checkpoints) {
        try {
            K8sManager.applyYaml(yaml);
        } catch (RuntimeException failure) {
            for (Checkpoint checkpoint : checkpoints) {
                try {
                    // A partial submission still needs cleanup on retry. Keep its YAML and
                    // artifacts, and only fail this attempt, never a concurrent replacement.
                    db.completeCheckpoint(checkpoint, Consts.CHECKPOINT_STATUS_FAILED);
                } catch (RuntimeException statusFailure) {
                    failure.addSuppressed(statusFailure);
                }
            }
            throw failure;
        }
    }

    static Model getRequiredModel(MetadataAccess db, String modelName) {
        Model model = db.getModel(modelName);
        if (model == null) {
            throw new IllegalArgumentException("model not exists: " + modelName);
        }
        return model;
    }

    /** Existing checkpoints retain the model path recorded when they were created. */
    static String checkpointPath(ModelConf model, Checkpoint checkpoint, String checkpointName) throws Exception {
        return checkpoint == null ? PathUtils.checkpointPath(model.getPath(), checkpointName)
                : ModelEntityConverter.getModelCheckpointPath(checkpoint);
    }

    private static List<String> loadTrainingDataPaths(
            MetadataAccess metadata, SqlIdentifier dataSource, SqlNode whereCondition, String defaultSchema
    ) throws Exception {
        if (dataSource == null) {
            return List.of();
        }
        String database = dataSource.isSimple() ? defaultSchema : dataSource.getComponent(0).toString();
        String table = dataSource.isSimple() ? dataSource.toString() : dataSource.getComponent(1).toString();
        String filter = whereCondition == null ? "" : whereCondition.toString();
        List<String> paths = metadata.getPartitionPaths(database, table, filter);
        return ModelEntityConverter.fixPathProtocol(paths).stream().map(path -> path + "/*").toList();
    }

    private static ExportCheckpoints findExportCheckpoints(
            MetadataAccess db, String modelName, String modelPath, List<String> checkpointNames) {
        List<Checkpoint> existing = new ArrayList<>();
        List<CheckpointInfo> inProgress = new ArrayList<>();
        for (String name : checkpointNames) {
            String path = PathUtils.checkpointPath(modelPath, name);
            Checkpoint checkpoint = db.getCheckpoint(modelName, name);
            if (checkpoint == null) {
                validateNewCheckpointPath(db, path);
                continue;
            }
            if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
                throw new IllegalArgumentException("Export target is not an export checkpoint: " + name);
            }
            existing.add(checkpoint);
        }
        // Validate every target's type before consulting Jobs or changing checkpoint status.
        for (Checkpoint checkpoint : existing) {
            if (Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                    && CheckpointCompletion.canReuseCreated(db, checkpoint)) {
                log.info("Model {} has export checkpoint {} in progress", modelName, checkpoint.getCheckpointName());
                inProgress.add(new CheckpointInfo(checkpoint.getModelName(), checkpoint.getCheckpointName()));
            }
        }
        return new ExportCheckpoints(existing, inProgress);
    }

    private static void validateNewCheckpointPath(MetadataAccess db, String path) {
        if (db.hdfsPathExists(path)) {
            throw new IllegalArgumentException("Checkpoint metadata does not exist, but path already exists: " + path);
        }
    }

    private static void deleteExportCheckpoints(MetadataAccess db, String modelName, List<Checkpoint> checkpoints) throws Exception {
        Set<String> oldYamls = new LinkedHashSet<>();
        // Preflight every output before deleting the first one.
        for (Checkpoint checkpoint : checkpoints) {
            validateCheckpointModification(db, checkpoint);
            if (StringUtils.isNotEmpty(checkpoint.getYaml())) {
                oldYamls.add(checkpoint.getYaml());
            }
        }
        // Succeeded Jobs may still exist; stop shared Jobs once before removing any artifacts.
        for (String yaml : oldYamls) {
            K8sManager.deleteYamlAndWait(yaml);
        }
        for (Checkpoint checkpoint : checkpoints) {
            log.info("Model {} re export checkpoint {}, deleting old first",
                    modelName, checkpoint.getCheckpointName());
            db.hdfsDeletePath(validateCheckpointModification(db, checkpoint));
            db.deleteCheckpoint(checkpoint.getModelName(), checkpoint.getCheckpointName());
        }
    }

    private record ExportCheckpoints(List<Checkpoint> existing, List<CheckpointInfo> inProgress) {
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

        deleteCheckpoint(db, checkpoint);
    }

    private static void deleteCheckpoint(MetadataAccess db, Checkpoint checkpoint) throws Exception {
        String checkpointPath = validateCheckpointModification(db, checkpoint);
        String status = checkpoint.getStatus();
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(status)) {
            String k8sYaml = checkpoint.getYaml();
            if (!StringUtils.isEmpty(k8sYaml)) {
                K8sManager.deleteYamlAndWait(k8sYaml);
            }
        }

        db.hdfsDeletePath(checkpointPath);

        db.deleteCheckpoint(checkpoint.getModelName(), checkpoint.getCheckpointName());
    }

    private static String validateCheckpointModification(MetadataAccess db, Checkpoint checkpoint) throws Exception {
        String modelName = checkpoint.getModelName();
        String checkpointName = checkpoint.getCheckpointName();
        List<Service> services = db.getServiceListByCheckpoint(modelName, checkpointName);
        if (!services.isEmpty()) {
            throw new IllegalArgumentException("Cannot modify checkpoint " + checkpointName + " for model " + modelName +
                    " because it is being used by " + services.size() + " service(s): " +
                    String.join(", ", services.stream().map(Service::getName).toList()));
        }

        return ModelEntityConverter.getModelCheckpointPath(checkpoint);
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
            deleteCheckpoint(db, checkpoint);
        }

        ModelConf modelConfig = ModelEntityConverter.convertToModel(model.getDdl());
        db.hdfsDeletePath(modelConfig.getPath());

        db.deleteModel(modelName);
    }

    public static CompletableFuture<Void> watchCheckpointCompletion(List<CheckpointInfo> checkpointInfos) {
        return watchCheckpointCompletion(checkpointInfos, ExecutorServiceUtils.getExecutorService());
    }

    static CompletableFuture<Void> watchCheckpointCompletion(List<CheckpointInfo> checkpointInfos, Executor executor) {
        return CheckpointCompletion.watch(checkpointInfos, executor);
    }
}
