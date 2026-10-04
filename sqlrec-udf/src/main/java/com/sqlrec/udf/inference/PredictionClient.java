package com.sqlrec.udf.inference;

import com.sqlrec.common.http.JsonHttpTransport;
import com.sqlrec.common.http.JdkJsonHttpTransport;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** HTTP prediction protocol shared by online and Flink batch functions. */
public final class PredictionClient {
    // Flink's Hadoop uber-jar embeds Okio 1.x. The default prediction path
    // must not initialize OkHttp 4, regardless of the shared jar load order.
    private static final PredictionClient DEFAULT = new PredictionClient();
    private static final int DEFAULT_TIMEOUT_MS = 30000;

    private final OkHttpClient httpClient;

    private PredictionClient() {
        this.httpClient = null;
    }

    /** Retains support for callers that explicitly supply an OkHttp client. */
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
        try {
            String responseBody;
            if (httpClient == null) {
                responseBody = JdkJsonHttpTransport.post(serviceUrl, jsonData,
                        timeout(serviceParams, "connect_timeout_ms"),
                        timeout(serviceParams, "read_timeout_ms"),
                        timeout(serviceParams, "write_timeout_ms"));
            } else {
                responseBody = JsonHttpTransport.post(clientWithServiceTimeouts(serviceParams), serviceUrl, jsonData);
            }
            return JsonUtils.parseJsonToMap(responseBody);
        } catch (IOException e) {
            throw new RuntimeException("Failed to call prediction service: " + e.getMessage(), e);
        }
    }

    private static int timeout(Map<String, String> params, String key) {
        if (params == null || !params.containsKey(key)) {
            return DEFAULT_TIMEOUT_MS;
        }
        long value = parsePositiveTimeout(params, key);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + " is too large");
        }
        return (int) value;
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
