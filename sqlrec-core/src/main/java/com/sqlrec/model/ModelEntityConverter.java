package com.sqlrec.model;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelTrainConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelEntityConverter {
    private static final Logger log = LoggerFactory.getLogger(ModelEntityConverter.class);

    public static ModelConfig convertToModel(SqlCreateModel sqlCreateModel) {
        ModelConfig model = new ModelConfig();
        model.modelName = sqlCreateModel.getModelName().toString();
        model.fieldSchemas = convertFieldList(sqlCreateModel.getFieldList());
        model.params = convertPropertyList(sqlCreateModel.getPropertyList());
        return model;
    }

    public static ModelTrainConf convertToModelTrainConf(SqlTrainModel sqlTrainModel) {
        ModelTrainConf modelTrainConf = new ModelTrainConf();
        modelTrainConf.name = sqlTrainModel.getModelName().toString();
        modelTrainConf.params = convertPropertyList(sqlTrainModel.getPropertyList());
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
}
