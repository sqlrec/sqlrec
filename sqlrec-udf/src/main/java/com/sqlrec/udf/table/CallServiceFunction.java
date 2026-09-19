package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.*;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CallServiceFunction {
    private static final OkHttpClient DEFAULT_HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();
    private final OkHttpClient httpClient;

    public CallServiceFunction() {
        this(DEFAULT_HTTP_CLIENT);
    }

    public CallServiceFunction(OkHttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable input) {
        ResolvedService service = resolveService(context, serviceName);
        ServiceConf serviceConfig = service.config;
        List<FieldSchema> modelOutputFields = service.outputFields;
        List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(input.getDataFields(), modelOutputFields);

        List<Object[]> inputData = DataTransformUtils.materializeRows(input);
        if (inputData.isEmpty()) {
            return resultTable(inputData, newDataFields);
        }

        List<FieldSchema> inputFields = serviceConfig.getModelConfig().getInputFields();
        String jsonData = JsonUtils.toJsonArray(inputData, inputFields, input.getDataFields());

        Map<String, Object> predictions = callPredictionService(
                httpClient,
                serviceConfig.getUrl(), jsonData, serviceConfig.getParams());

        List<Object[]> newData = mergePredictions(inputData, predictions, modelOutputFields);

        return resultTable(newData, newDataFields);
    }

    public CacheTable evaluate(ReadonlyContext context, String serviceName, CacheTable user, CacheTable item) {
        ResolvedService service = resolveService(context, serviceName);
        ServiceConf serviceConfig = service.config;
        List<FieldSchema> modelOutputFields = service.outputFields;

        List<Object[]> userData = DataTransformUtils.materializeRows(user);
        if (userData.size() != 1) {
            throw new RuntimeException("User table must have exactly one row");
        }

        List<Object[]> itemData = DataTransformUtils.materializeRows(item);
        if (itemData.isEmpty()) {
            List<RelDataTypeField> newDataFields = DataTypeUtils.addTypeFields(
                    item.getDataFields(),
                    modelOutputFields
            );
            return resultTable(itemData, newDataFields);
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
                httpClient,
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
        return callPredictionService(DEFAULT_HTTP_CLIENT, serviceUrl, jsonData, null);
    }

    public static Map<String, Object> callPredictionService(
            String serviceUrl, String jsonData, Map<String, String> serviceParams) {
        return callPredictionService(DEFAULT_HTTP_CLIENT, serviceUrl, jsonData, serviceParams);
    }

    static Map<String, Object> callPredictionService(
            OkHttpClient httpClient,
            String serviceUrl,
            String jsonData) {
        return callPredictionService(httpClient, serviceUrl, jsonData, null);
    }

    static Map<String, Object> callPredictionService(
            OkHttpClient httpClient,
            String serviceUrl,
            String jsonData,
            Map<String, String> serviceParams) {
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

            OkHttpClient client = clientWithServiceTimeouts(httpClient, serviceParams);
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

    private static OkHttpClient clientWithServiceTimeouts(
            OkHttpClient httpClient,
            Map<String, String> params) {
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

    public static List<Object[]> mergePredictions(
            List<Object[]> inputData,
            Map<String, Object> predictions,
            List<FieldSchema> outputFields) {
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

    private static final class ResolvedService {
        private final ServiceConf config;
        private final List<FieldSchema> outputFields;

        private ResolvedService(ServiceConf config, List<FieldSchema> outputFields) {
            this.config = config;
            this.outputFields = outputFields;
        }
    }
}
