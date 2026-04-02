package com.sqlrec.model;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.schema.HmsClient;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ModelEntityConverter {
    private static final Logger log = LoggerFactory.getLogger(ModelEntityConverter.class);

    public static ModelConfig convertToModel(String modelDdl) throws Exception {
        SqlNode modelSqlNode = CompileManager.parseFlinkSql(modelDdl);
        if (!(modelSqlNode instanceof SqlCreateModel)) {
            throw new IllegalArgumentException("Invalid model DDL: " + modelDdl);
        }
        return convertToModel((SqlCreateModel) modelSqlNode);
    }

    public static ModelConfig convertToModel(SqlCreateModel sqlCreateModel) {
        ModelConfig modelConfig = new ModelConfig();
        modelConfig.modelName = sqlCreateModel.getModelName().toString();
        modelConfig.inputFields = SchemaUtils.convertFieldList(sqlCreateModel.getFieldList());
        modelConfig.params = SchemaUtils.convertPropertyList(sqlCreateModel.getPropertyList());
        modelConfig.path = getModelPath(modelConfig);

        if (!modelConfig.params.containsKey(ModelConfigs.MODEL_PATH.getKey())) {
            modelConfig.params.put(ModelConfigs.MODEL_PATH.getKey(), modelConfig.path);
            sqlCreateModel.setPropertyList(
                    SchemaUtils.addConfigToPropertyList(
                            sqlCreateModel.getPropertyList(),
                            ModelConfigs.MODEL_PATH.getKey(),
                            modelConfig.path
                    )
            );
        }

        modelConfig.ddl = sqlCreateModel.toString();
        return modelConfig;
    }

    public static ModelTrainConf convertToModelTrainConf(SqlTrainModel sqlTrainModel, String defaultSchema) throws Exception {
        ModelTrainConf modelTrainConf = new ModelTrainConf();
        modelTrainConf.modelName = sqlTrainModel.getModelName().toString();
        modelTrainConf.checkpointName = SchemaUtils.removeQuotes(sqlTrainModel.getCheckpoint().toString());
        modelTrainConf.modelDir = ModelEntityConverter.getModelCheckpointPath(
                modelTrainConf.modelName, modelTrainConf.checkpointName);
        if (sqlTrainModel.getExistingCheckpoint() != null) {
            modelTrainConf.baseModelDir = ModelEntityConverter.getModelCheckpointPath(
                    modelTrainConf.modelName,
                    SchemaUtils.removeQuotes(sqlTrainModel.getExistingCheckpoint().toString())
            );
        }
        modelTrainConf.params = SchemaUtils.convertPropertyList(sqlTrainModel.getPropertyList());
        modelTrainConf.id = K8sManager.convertToValidK8sName(modelTrainConf.modelName + "-" + modelTrainConf.checkpointName);
        modelTrainConf.trainDataPaths = getHivePartitionPaths(sqlTrainModel.getDataSource(), sqlTrainModel.getWhereCondition(), defaultSchema);

        return modelTrainConf;
    }

    public static ModelExportConf convertToModelExportConf(SqlExportModel sqlExportModel, String defaultSchema) throws Exception {
        ModelExportConf modelExportConf = new ModelExportConf();
        modelExportConf.modelName = sqlExportModel.getModelName().toString();
        modelExportConf.checkpointName = SchemaUtils.removeQuotes(sqlExportModel.getCheckpoint().toString());
        modelExportConf.baseModelDir = ModelEntityConverter.getModelCheckpointPath(
                modelExportConf.modelName, modelExportConf.checkpointName);
        modelExportConf.params = SchemaUtils.convertPropertyList(sqlExportModel.getPropertyList());
        modelExportConf.id = K8sManager.convertToValidK8sName(modelExportConf.modelName + "-" + modelExportConf.checkpointName + "-export");
        modelExportConf.trainDataPaths = getHivePartitionPaths(sqlExportModel.getDataSource(), sqlExportModel.getWhereCondition(), defaultSchema);
        return modelExportConf;
    }

    public static ServiceConfig convertToServiceConf(SqlCreateService sqlCreateService) throws Exception {
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.id = K8sManager.convertToValidK8sName(sqlCreateService.getServiceName().toString());
        serviceConfig.serviceName = sqlCreateService.getServiceName().toString();
        serviceConfig.modelName = sqlCreateService.getModelName().toString();
        serviceConfig.checkpointName = SchemaUtils.removeQuotes(sqlCreateService.getCheckpoint().toString());
        serviceConfig.modelCheckpointDir = getModelCheckpointPath(serviceConfig.modelName, serviceConfig.checkpointName);
        serviceConfig.params = SchemaUtils.convertPropertyList(sqlCreateService.getPropertyList());
        return serviceConfig;
    }

    public static ServiceConfig convertToServiceConfig(Service service) throws Exception {
        SqlNode modelSqlNode = CompileManager.parseFlinkSql(service.getDdl());
        if (!(modelSqlNode instanceof SqlCreateService)) {
            throw new IllegalArgumentException("Invalid service DDL: " + service.getDdl());
        }
        ServiceConfig serviceConfig = convertToServiceConf((SqlCreateService) modelSqlNode);
        serviceConfig.url = service.getUrl();
        serviceConfig.modelConfig = convertToModel(service.getModelDdl());
        return serviceConfig;
    }

    public static List<String> getHivePartitionPaths(SqlIdentifier dataSource, SqlNode whereCondition, String defaultSchema) throws Exception {
        String db = defaultSchema;
        String table = null;
        String partitionFilter = "";
        if (whereCondition != null) {
            partitionFilter = whereCondition.toString();
        }
        if (dataSource.isSimple()) {
            table = dataSource.toString();
        } else {
            db = dataSource.getComponent(0).toString();
            table = dataSource.getComponent(1).toString();
        }
        List<String> partitionPaths = HmsClient.getPartitionPaths(db, table, partitionFilter);
        partitionPaths = fixPathProtocol(partitionPaths);
        return partitionPaths.stream().map(path -> path + "/*").toList();
    }

    public static String getModelPath(ModelConfig modelConfig) {
        if (StringUtils.isEmpty(modelConfig.modelName)) {
            throw new RuntimeException("modelName is empty");
        }

        if (modelConfig.params.containsKey(ModelConfigs.MODEL_PATH.getKey())) {
            return modelConfig.params.get(ModelConfigs.MODEL_PATH.getKey());
        }

        String fullPath = ModelConfigs.MODEL_BASE_PATH.getValue();
        if (!fullPath.endsWith("/")) {
            fullPath += "/";
        }
        fullPath += modelConfig.modelName;

        List<String> fixedPaths = fixPathProtocol(Collections.singletonList(fullPath));
        return fixedPaths.get(0);
    }

    public static String getModelCheckpointPath(String modelName, String checkpoint) throws Exception {
        String modelDdl = null;
        Checkpoint checkpointEntity = DbUtils.getCheckpoint(modelName, checkpoint);
        if (checkpointEntity != null) {
            modelDdl = checkpointEntity.getModelDdl();
        } else {
            Model model = DbUtils.getModel(modelName);
            if (model == null) {
                throw new IllegalArgumentException("model not exists: " + modelName);
            }
            modelDdl = model.getDdl();
        }

        ModelConfig modelConfig = convertToModel(modelDdl);
        return modelConfig.path + "/" + checkpoint;
    }

    public static String getModelCheckpointPath(Checkpoint checkpointEntity) throws Exception {
        ModelConfig modelConfig = convertToModel(checkpointEntity.getModelDdl());
        return modelConfig.path + "/" + checkpointEntity.getCheckpointName();
    }

    public static List<String> fixPathProtocol(List<String> partitionPaths) {
        String defaultFS = null;
        List<String> r = new ArrayList<>();
        for (String path : partitionPaths) {
            if (!path.contains(":")) {
                if (defaultFS == null) {
                    Configuration hadoopConf = new Configuration();
                    defaultFS = hadoopConf.get("fs.defaultFS", "hdfs:///");
                    log.info("Default filesystem: {}", defaultFS);
                }
                r.add(defaultFS + path);
            } else {
                r.add(path);
            }
        }
        return r;
    }
}
