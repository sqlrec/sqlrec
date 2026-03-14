package com.sqlrec.model;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelTrainConf;
import com.sqlrec.schema.HmsClient;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ModelEntityConverter {
    private static final Logger log = LoggerFactory.getLogger(ModelEntityConverter.class);

    public static ModelConfig convertToModel(SqlCreateModel sqlCreateModel) {
        ModelConfig model = new ModelConfig();
        model.modelName = sqlCreateModel.getModelName().toString();
        model.fieldSchemas = convertFieldList(sqlCreateModel.getFieldList());
        model.params = convertPropertyList(sqlCreateModel.getPropertyList());
        return model;
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
        modelTrainConf.params = convertPropertyList(sqlTrainModel.getPropertyList());
        modelTrainConf.id = K8sManager.convertToValidK8sName(modelTrainConf.modelName + "-" + modelTrainConf.checkpointName);

        String db = defaultSchema;
        String table = null;
        String partitionFilter = "";
        if (sqlTrainModel.getWhereCondition() != null) {
            partitionFilter = sqlTrainModel.getWhereCondition().toString();
        }
        if (sqlTrainModel.getDataSource().isSimple()) {
            table = sqlTrainModel.getDataSource().toString();
        } else {
            db = sqlTrainModel.getDataSource().getComponent(0).toString();
            table = sqlTrainModel.getDataSource().getComponent(1).toString();
        }
        List<String> partitionPaths = HmsClient.getPartitionPaths(db, table, partitionFilter);
        partitionPaths = fixPathProtocol(partitionPaths);
        modelTrainConf.trainDataPaths = partitionPaths.stream().map(path -> path + "/*").toList();

        return modelTrainConf;
    }

    public static List<FieldSchema> convertFieldList(SqlNodeList fieldList) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        if (fieldList != null && fieldList.size() > 0) {
            for (SqlNode field : fieldList) {
                FieldSchema fieldSchema = convertField(field);
                if (fieldSchema != null) {
                    fieldSchemas.add(fieldSchema);
                }
            }
        }
        return fieldSchemas;
    }

    public static FieldSchema convertField(SqlNode field) {
        if (field == null) {
            return null;
        }

        if (field instanceof SqlTableColumn.SqlRegularColumn) {
            SqlTableColumn.SqlRegularColumn regularColumn = (SqlTableColumn.SqlRegularColumn) field;
            String name = regularColumn.getName().toString();
            String type = regularColumn.getType().toString();
            return new FieldSchema(name, type);
        } else {
            log.warn("Unsupported field type: {}", field);
            throw new IllegalArgumentException("Unsupported field type: " + field);
        }
    }

    public static Map<String, String> convertPropertyList(SqlNodeList propertyList) {
        Map<String, String> params = new HashMap<>();
        if (propertyList != null && propertyList.size() > 0) {
            for (SqlNode property : propertyList) {
                if (property instanceof SqlTableOption) {
                    SqlTableOption option = (SqlTableOption) property;
                    String key = SchemaUtils.removeQuotes(option.getKey().toString());
                    String value = SchemaUtils.removeQuotes(option.getValue().toString());
                    params.put(key, value);
                } else {
                    log.warn("Unsupported property type: {}", property);
                    throw new IllegalArgumentException("Unsupported property type: " + property);
                }
            }
        }
        return params;
    }

    public static String getModelCheckpointPath(String modelName, String checkpoint) {
        String modelBasePath = ModelConfigs.MODEL_BASE_PATH.getValue();
        log.info("MODEL_BASE_PATH: {}", modelBasePath);

        String fullPath = modelBasePath;
        if (!fullPath.endsWith("/")) {
            fullPath += "/";
        }
        fullPath += modelName + "/" + checkpoint;

        List<String> fixedPaths = fixPathProtocol(Collections.singletonList(fullPath));
        return fixedPaths.get(0);
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
