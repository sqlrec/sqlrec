package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CallServiceFunction {
    private static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    public CacheTable eval(ExecuteContext context, String serviceName, CacheTable input) {
        ServiceConfig serviceConfig = context.getServiceConfig(serviceName);
        if (serviceConfig == null) {
            throw new RuntimeException("Service " + serviceName + " not exist or formate error");
        }
        if (StringUtils.isEmpty(serviceConfig.getUrl())) {
            throw new RuntimeException("Service " + serviceName + " url is empty");
        }
        ModelController controller = context.getModelController(serviceConfig.getModelConfig());
        if (controller == null){
            throw new RuntimeException("model controller not exist for " +serviceName);
        }
        List<FieldSchema> modelOutputFields = controller.getOutputFields(serviceConfig.getModelConfig());
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(input.getDataFields(), modelOutputFields);

        Enumerable<Object[]> enumerable = input.scan(null);
        if (enumerable == null || enumerable.count() == 0 ) {
            return new CacheTable("output", Linq4j.asEnumerable(new ArrayList<>()), newDataFields);
        }

        List<Object[]> inputData = new ArrayList<>();
        for (Object[] row : enumerable) {
            inputData.add(row);
        }

        List<FieldSchema> inputFields = serviceConfig.getModelConfig().getInputFields();
        String jsonData = JsonUtils.toJsonArray(inputData, inputFields, input.getDataFields());
        
        Map<String, Object> predictions = callPredictionService(serviceConfig.getUrl(), jsonData);
        
        List<Object[]> newData = mergePredictions(inputData, predictions, modelOutputFields);

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }

    public static Map<String, Object> callPredictionService(String serviceUrl, String jsonData) {
        try {
            RequestBody body = RequestBody.create(
                    jsonData, 
                    MediaType.parse("application/json; charset=utf-8")
            );
            
            Request request = new Request.Builder()
                    .url(serviceUrl)
                    .post(body)
                    .addHeader("Accept", "application/json")
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new RuntimeException("HTTP request failed with response code: " + response.code());
                }

                String responseBody = response.body() != null ? response.body().string() : "";
                return JsonUtils.parseJsonToMap(responseBody);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to call prediction service: " + e.getMessage(), e);
        }
    }

    public static List<Object[]> mergePredictions(List<Object[]> inputData, Map<String, Object> predictions, List<FieldSchema> outputFields) {
        List<Object[]> newData = new ArrayList<>();
        
        for (int i = 0; i < inputData.size(); i++) {
            Object[] inputRow = inputData.get(i);
            Object[] newRow = new Object[inputRow.length + outputFields.size()];
            System.arraycopy(inputRow, 0, newRow, 0, inputRow.length);
            
            for (int j = 0; j < outputFields.size(); j++) {
                FieldSchema field = outputFields.get(j);
                Object prediction = predictions.get(field.getName());
                if (prediction instanceof List) {
                    List<?> predictionList = (List<?>) prediction;
                    if (i < predictionList.size()) {
                        newRow[inputRow.length + j] = predictionList.get(i);
                    }
                } else {
                    newRow[inputRow.length + j] = prediction;
                }
            }
            
            newData.add(newRow);
        }
        
        return newData;
    }
}
