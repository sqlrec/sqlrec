package com.sqlrec.model;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sYamlUtils;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.PathUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/** Converts SQL and stored entities; managers resolve metadata and execution paths. */
public class ModelEntityConverter {
    public static ModelConf convertToModel(String modelDdl) throws Exception {
        SqlNode modelSqlNode = CompileManager.parseSql(modelDdl);
        if (!(modelSqlNode instanceof SqlCreateModel createModel)) {
            throw new IllegalArgumentException("Invalid model DDL: " + modelDdl);
        }
        return convertToModel(createModel);
    }

    public static ModelConf convertToModel(SqlCreateModel sqlCreateModel) {
        ModelConf modelConfig = new ModelConf();
        modelConfig.setModelName(ResourceNames.of(sqlCreateModel.getModelName()));
        modelConfig.setInputFields(SchemaUtils.convertFieldList(sqlCreateModel.getFieldList()));
        modelConfig.setParams(SchemaUtils.convertPropertyList(sqlCreateModel.getPropertyList()));
        modelConfig.setPath(getModelPath(modelConfig));

        if (!modelConfig.getParams().containsKey(ModelConfigs.MODEL_PATH.getKey())) {
            modelConfig.getParams().put(ModelConfigs.MODEL_PATH.getKey(), modelConfig.getPath());
            sqlCreateModel.setPropertyList(
                    SchemaUtils.addConfigToPropertyList(
                            sqlCreateModel.getPropertyList(),
                            ModelConfigs.MODEL_PATH.getKey(),
                            modelConfig.getPath()
                    )
            );
        }

        modelConfig.setDdl(sqlCreateModel.toString());
        return modelConfig;
    }

    public static ModelTrainConf convertToModelTrainConf(SqlTrainModel sqlTrainModel) {
        ModelTrainConf modelTrainConf = new ModelTrainConf();
        modelTrainConf.setModelName(ResourceNames.of(sqlTrainModel.getModelName()));
        modelTrainConf.setCheckpointName(checkpointName(sqlTrainModel.getCheckpoint()));
        modelTrainConf.setParams(SchemaUtils.convertPropertyList(sqlTrainModel.getPropertyList()));
        modelTrainConf.setId(K8sYamlUtils.checkpointJobId("train", modelTrainConf.getModelName(), modelTrainConf.getCheckpointName()));
        return modelTrainConf;
    }

    public static ModelExportConf convertToModelExportConf(SqlExportModel sqlExportModel) {
        ModelExportConf modelExportConf = new ModelExportConf();
        modelExportConf.setModelName(ResourceNames.of(sqlExportModel.getModelName()));
        modelExportConf.setCheckpointName(checkpointName(sqlExportModel.getCheckpoint()));
        modelExportConf.setParams(SchemaUtils.convertPropertyList(sqlExportModel.getPropertyList()));
        modelExportConf.setId(K8sYamlUtils.checkpointJobId("export", modelExportConf.getModelName(), modelExportConf.getCheckpointName()));
        return modelExportConf;
    }

    public static ServiceConf convertToServiceConf(SqlCreateService sqlCreateService) throws Exception {
        ServiceConf serviceConfig = new ServiceConf();
        String serviceName = K8sYamlUtils.serviceResourceId(sqlCreateService.getServiceName().getSimple());
        serviceConfig.setServiceName(serviceName);
        serviceConfig.setId(serviceName);
        serviceConfig.setModelName(ResourceNames.of(sqlCreateService.getModelName()));
        if (sqlCreateService.getCheckpoint() != null) {
            serviceConfig.setCheckpointName(checkpointName(sqlCreateService.getCheckpoint()));
        }
        serviceConfig.setParams(SchemaUtils.convertPropertyList(sqlCreateService.getPropertyList()));
        return serviceConfig;
    }

    private static String checkpointName(SqlNode node) {
        String name = ResourceNames.normalize(SchemaUtils.removeQuotes(node.toString()));
        PathUtils.validateCheckpointName(name);
        return name;
    }

    public static ServiceConf convertToServiceConfig(Service service) throws Exception {
        SqlNode serviceSqlNode = CompileManager.parseSql(service.getDdl());
        if (!(serviceSqlNode instanceof SqlCreateService createService)) {
            throw new IllegalArgumentException("Invalid service DDL: " + service.getDdl());
        }
        ServiceConf serviceConfig = convertToServiceConf(createService);
        serviceConfig.setUrl(service.getUrl());
        serviceConfig.setModelConfig(convertToModel(service.getModelDdl()));
        return serviceConfig;
    }

    public static String getModelPath(ModelConf modelConfig) {
        if (StringUtils.isEmpty(modelConfig.getModelName())) {
            throw new RuntimeException("modelName is empty");
        }

        if (modelConfig.getParams().containsKey(ModelConfigs.MODEL_PATH.getKey())) {
            return modelConfig.getParams().get(ModelConfigs.MODEL_PATH.getKey());
        }

        String fullPath = ModelConfigs.MODEL_BASE_PATH.getValue();
        if (!fullPath.endsWith("/")) {
            fullPath += "/";
        }
        fullPath += modelConfig.getModelName();

        List<String> fixedPaths = PathUtils.fixPathProtocol(Collections.singletonList(fullPath));
        return fixedPaths.get(0);
    }

    public static String getModelCheckpointPath(Checkpoint checkpointEntity) throws Exception {
        ModelConf modelConfig = convertToModel(checkpointEntity.getModelDdl());
        return PathUtils.checkpointPath(modelConfig.getPath(), checkpointEntity.getCheckpointName());
    }

    public static List<String> fixPathProtocol(List<String> partitionPaths) {
        return PathUtils.fixPathProtocol(partitionPaths);
    }
}
