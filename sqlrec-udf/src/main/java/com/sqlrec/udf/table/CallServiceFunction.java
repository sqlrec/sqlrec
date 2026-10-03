package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.inference.PredictionClient;
import com.sqlrec.udf.inference.PredictionRequestEncoder;
import com.sqlrec.udf.inference.PredictionResult;
import okhttp3.OkHttpClient;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CallServiceFunction {
    private final PredictionClient predictionClient;

    public CallServiceFunction() {
        this.predictionClient = PredictionClient.getDefault();
    }

    public CallServiceFunction(OkHttpClient httpClient) {
        this.predictionClient = new PredictionClient(httpClient);
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable input) {
        ResolvedService service = resolveService(context, serviceName);
        ServiceConf serviceConfig = service.config;
        List<FieldSchema> modelOutputFields = service.outputFields;
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(input.getDataFields(), modelOutputFields);

        List<Object[]> inputData = RowTransformUtils.materializeRows(input);
        if (inputData.isEmpty()) {
            return resultTable(inputData, newDataFields);
        }

        List<FieldSchema> inputFields = serviceConfig.getModelConfig().getInputFields();
        String jsonData = PredictionRequestEncoder.encodeRows(inputData, inputFields, input.getDataFields());

        Map<String, Object> predictions = predictionClient.predict(
                serviceConfig.getUrl(), jsonData, serviceConfig.getParams());

        List<Object[]> newData = mergePredictions(inputData, predictions, modelOutputFields);

        return resultTable(newData, newDataFields);
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable user, CacheTable item) {
        ResolvedService service = resolveService(context, serviceName);
        ServiceConf serviceConfig = service.config;
        List<FieldSchema> modelOutputFields = service.outputFields;

        List<Object[]> userData = RowTransformUtils.materializeRows(user);
        if (userData.size() != 1) {
            throw new RuntimeException("User table must have exactly one row");
        }

        List<Object[]> itemData = RowTransformUtils.materializeRows(item);
        if (itemData.isEmpty()) {
            List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(
                    item.getDataFields(),
                    modelOutputFields
            );
            return resultTable(itemData, newDataFields);
        }

        String jsonData = PredictionRequestEncoder.encodeUserItems(userData, itemData,
                serviceConfig.getModelConfig().getInputFields(), user.getDataFields(), item.getDataFields());

        Map<String, Object> predictions = predictionClient.predict(
                serviceConfig.getUrl(), jsonData, serviceConfig.getParams());
        List<Object[]> newData = mergePredictions(itemData, predictions, modelOutputFields);
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(
                item.getDataFields(),
                modelOutputFields
        );

        return resultTable(newData, newDataFields);
    }

    private static ResolvedService resolveService(ReadonlyContext context, String serviceName) {
        ServiceConf serviceConfig = context.getServiceConfig(serviceName);
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
        return new ResolvedService(
                serviceConfig,
                controller.getOutputFields(serviceConfig.getModelConfig())
        );
    }

    private static CacheTable resultTable(
            List<Object[]> rows,
            List<RelDataTypeField> fields
    ) {
        return new CacheTable("output", Linq4j.asEnumerable(rows), fields);
    }

    public static Map<String, Object> callPredictionService(String serviceUrl, String jsonData) {
        return PredictionClient.getDefault().predict(serviceUrl, jsonData);
    }

    public static Map<String, Object> callPredictionService(
            String serviceUrl, String jsonData, Map<String, String> serviceParams) {
        return PredictionClient.getDefault().predict(serviceUrl, jsonData, serviceParams);
    }

    static Map<String, Object> callPredictionService(
            OkHttpClient httpClient, String serviceUrl, String jsonData) {
        return callPredictionService(httpClient, serviceUrl, jsonData, null);
    }

    static Map<String, Object> callPredictionService(
            OkHttpClient httpClient, String serviceUrl, String jsonData, Map<String, String> serviceParams) {
        return new PredictionClient(httpClient).predict(serviceUrl, jsonData, serviceParams);
    }

    public static List<Object[]> mergePredictions(
            List<Object[]> inputData,
            Map<String, Object> predictions,
            List<FieldSchema> outputFields) {
        List<Object[]> newData = new ArrayList<>();
        PredictionResult result = new PredictionResult(predictions);

        for (int i = 0; i < inputData.size(); i++) {
            Object[] inputRow = inputData.get(i);
            Object[] newRow = new Object[inputRow.length + outputFields.size()];
            System.arraycopy(inputRow, 0, newRow, 0, inputRow.length);

            for (int j = 0; j < outputFields.size(); j++) {
                FieldSchema field = outputFields.get(j);
                newRow[inputRow.length + j] = result.getValue(field.getName(), i);
            }

            newData.add(newRow);
        }

        return newData;
    }

    private static final class ResolvedService {
        private final ServiceConf config;
        private final List<FieldSchema> outputFields;

        private ResolvedService(ServiceConf config, List<FieldSchema> outputFields) {
            this.config = config;
            this.outputFields = outputFields;
        }
    }
}
