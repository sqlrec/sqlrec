package com.sqlrec.udf.inference;

import com.sqlrec.common.http.JsonHttpTransport;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** HTTP prediction protocol shared by online and Flink batch functions. */
public final class PredictionClient {
    private static final PredictionClient DEFAULT = new PredictionClient(new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build());

    private final OkHttpClient httpClient;

    public PredictionClient(OkHttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    public static PredictionClient getDefault() {
        return DEFAULT;
    }

    public Map<String, Object> predict(String serviceUrl, String jsonData) {
        return predict(serviceUrl, jsonData, null);
    }

    public Map<String, Object> predict(
            String serviceUrl, String jsonData, Map<String, String> serviceParams) {
        OkHttpClient client = clientWithServiceTimeouts(serviceParams);
        try {
            String responseBody = JsonHttpTransport.post(client, serviceUrl, jsonData);
            return JsonUtils.parseJsonToMap(responseBody);
        } catch (IOException e) {
            throw new RuntimeException("Failed to call prediction service: " + e.getMessage(), e);
        }
    }

    private OkHttpClient clientWithServiceTimeouts(Map<String, String> params) {
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
}
