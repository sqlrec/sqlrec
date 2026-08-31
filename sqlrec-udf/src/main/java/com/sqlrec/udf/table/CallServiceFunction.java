package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
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
    private static volatile OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    /**
     * Test-only: inject a mock OkHttpClient.
     */
    static void setHttpClientForTest(OkHttpClient mockClient) {
        httpClient = mockClient;
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable input) {
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
        List<FieldSchema> modelOutputFields = controller.getOutputFields(serviceConfig.getModelConfig());
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(input.getDataFields(), modelOutputFields);

        Enumerable<Object[]> enumerable = input.scan(null);
        if (enumerable == null || enumerable.count() == 0) {
            return new CacheTable("output", Linq4j.asEnumerable(new ArrayList<>()), newDataFields);
        }

        List<Object[]> inputData = new ArrayList<>();
        for (Object[] row : enumerable) {
            inputData.add(row);
        }

        List<FieldSchema> inputFields = serviceConfig.getModelConfig().getInputFields();
        String jsonData = JsonUtils.toJsonArray(inputData, inputFields, input.getDataFields());

        Map<String, Object> predictions = callPredictionService(
                serviceConfig.getUrl(), jsonData, serviceConfig.getParams());

        List<Object[]> newData = mergePredictions(inputData, predictions, modelOutputFields);

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable user, CacheTable item) {
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

        List<FieldSchema> modelOutputFields = controller.getOutputFields(serviceConfig.getModelConfig());

        Enumerable<Object[]> userEnumerable = user.scan(null);
        List<Object[]> userData = new ArrayList<>();
        if (userEnumerable != null) {
            for (Object[] row : userEnumerable) {
                userData.add(row);
            }
        }
        if (userData.size() != 1) {
            throw new RuntimeException("User table must have exactly one row");
        }

        Enumerable<Object[]> itemEnumerable = item.scan(null);
        if (itemEnumerable == null || itemEnumerable.count() == 0) {
            List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(item.getDataFields(), modelOutputFields);
            return new CacheTable("output", Linq4j.asEnumerable(new ArrayList<>()), newDataFields);
        }

        List<Object[]> itemData = new ArrayList<>();
        for (Object[] row : itemEnumerable) {
            itemData.add(row);
        }

        List<FieldSchema> allInputFields = serviceConfig.getModelConfig().getInputFields();
        List<FieldSchema> userFields = new ArrayList<>();
        List<FieldSchema> itemFields = new ArrayList<>();
        for (FieldSchema field : allInputFields) {
            boolean foundInUser = false;
            for (RelDataTypeField dataField : user.getDataFields()) {
                if (dataField.getName().equalsIgnoreCase(field.getName())) {
                    foundInUser = true;
                    break;
                }
            }
            if (foundInUser) {
                userFields.add(field);
            } else {
                itemFields.add(field);
            }
        }

        String jsonData = JsonUtils.toColumnarJson(userData, itemData, userFields, itemFields,
                user.getDataFields(), item.getDataFields());

        Map<String, Object> predictions = callPredictionService(
                serviceConfig.getUrl(), jsonData, serviceConfig.getParams());
        List<Object[]> newData = mergePredictions(itemData, predictions, modelOutputFields);
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(item.getDataFields(), modelOutputFields);

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }

    public static Map<String, Object> callPredictionService(String serviceUrl, String jsonData) {
        return callPredictionService(serviceUrl, jsonData, null);
    }

    public static Map<String, Object> callPredictionService(
            String serviceUrl, String jsonData, Map<String, String> serviceParams) {
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

            OkHttpClient client = clientWithServiceTimeouts(serviceParams);
            try (Response response = client.newCall(request).execute()) {
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

    private static OkHttpClient clientWithServiceTimeouts(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return httpClient;
        }
        boolean hasOverride = params.containsKey("connect_timeout_ms")
                || params.containsKey("read_timeout_ms")
                || params.containsKey("write_timeout_ms");
        if (!hasOverride) {
            return httpClient;
        }
        OkHttpClient.Builder builder = httpClient.newBuilder();
        if (params.containsKey("connect_timeout_ms")) {
            builder.connectTimeout(parsePositiveTimeout(params, "connect_timeout_ms"), TimeUnit.MILLISECONDS);
        }
        if (params.containsKey("read_timeout_ms")) {
            builder.readTimeout(parsePositiveTimeout(params, "read_timeout_ms"), TimeUnit.MILLISECONDS);
        }
        if (params.containsKey("write_timeout_ms")) {
            builder.writeTimeout(parsePositiveTimeout(params, "write_timeout_ms"), TimeUnit.MILLISECONDS);
        }
        return builder.build();
    }

    private static long parsePositiveTimeout(Map<String, String> params, String key) {
        long value;
        try {
            value = Long.parseLong(params.get(key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a positive integer", e);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be a positive integer");
        }
        return value;
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
