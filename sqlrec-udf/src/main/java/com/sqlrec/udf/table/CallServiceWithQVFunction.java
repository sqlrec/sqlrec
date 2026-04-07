package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.runtime.ConfigContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CallServiceWithQVFunction {
    public CacheTable eval(ConfigContext context, String serviceName, CacheTable query, CacheTable value) {
        ServiceConfig serviceConfig = context.getServiceConfig(serviceName);
        if (serviceConfig == null) {
            throw new RuntimeException("Service " + serviceName + " not exist or formate error");
        }
        if (StringUtils.isEmpty(serviceConfig.getUrl())) {
            throw new RuntimeException("Service " + serviceName + " url is empty");
        }
        ModelController controller = context.getModelController(serviceConfig.getModelConfig());
        if (controller == null) {
            throw new RuntimeException("model controller not exist for " + serviceName);
        }

        List<FieldSchema> modelOutputFields = controller.getOutputFields(serviceConfig.getModelConfig());

        Enumerable<Object[]> queryEnumerable = query.scan(null);
        List<Object[]> queryData = new ArrayList<>();
        if (queryEnumerable != null) {
            for (Object[] row : queryEnumerable) {
                queryData.add(row);
            }
        }
        if (queryData.size() != 1) {
            throw new RuntimeException("Query table must have exactly one row");
        }

        Enumerable<Object[]> valueEnumerable = value.scan(null);
        if (valueEnumerable == null || valueEnumerable.count() == 0) {
            List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(value.getDataFields(), modelOutputFields);
            return new CacheTable("output", Linq4j.asEnumerable(new ArrayList<>()), newDataFields);
        }

        List<Object[]> valueData = new ArrayList<>();
        for (Object[] row : valueEnumerable) {
            valueData.add(row);
        }

        List<FieldSchema> allInputFields = serviceConfig.getModelConfig().getInputFields();

        List<FieldSchema> queryFields = new ArrayList<>();
        List<FieldSchema> valueFields = new ArrayList<>();
        for (FieldSchema field : allInputFields) {
            boolean foundInQuery = false;
            for (RelDataTypeField dataField : query.getDataFields()) {
                if (dataField.getName().equalsIgnoreCase(field.getName())) {
                    foundInQuery = true;
                    break;
                }
            }
            if (foundInQuery) {
                queryFields.add(field);
            } else {
                valueFields.add(field);
            }
        }

        String jsonData = JsonUtils.toColumnarJson(queryData, valueData, queryFields, valueFields,
                query.getDataFields(), value.getDataFields());

        Map<String, Object> predictions = CallServiceFunction.callPredictionService(serviceConfig.getUrl(), jsonData);

        List<Object[]> newData = CallServiceFunction.mergePredictions(valueData, predictions, modelOutputFields);

        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(value.getDataFields(), modelOutputFields);

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }
}
