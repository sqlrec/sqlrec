package com.sqlrec.common.rest;

import com.sqlrec.common.utils.JsonUtils;
import okhttp3.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Lightweight client for calling remote sqlrec REST APIs.
 *
 * <p>External code only needs to depend on the {@code sqlrec-common} module to
 * invoke remote sqlrec endpoints and obtain parsed results, without dealing with
 * HTTP transport or (de)serialization details. Two endpoint families are supported:
 *
 * <ul>
 *   <li>{@link #callFunctionApi} — function endpoints (e.g. {@code /api/v1/<name>}),
 *       request body matches {@link RequestData} ({@code {data, params, metricTags}}),
 *       response body matches {@link ExecuteData} ({@code {msg, data, params}}).</li>
 *   <li>{@link #callSqlApi} — the SQL endpoint ({@code /sql/v1}), request body carries
 *       {@code sqls}, response body matches {@link ExecuteDataList}
 *       (one {@link ExecuteData} per executed SQL statement).</li>
 * </ul>
 */
public final class SqlRecApiClient {
    private static volatile OkHttpClient HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.parse("application/json; charset=utf-8");

    private SqlRecApiClient() {
    }

    /**
     * Test-only: inject a mock OkHttpClient.
     */
    static void setHttpClientForTest(OkHttpClient mockClient) {
        HTTP_CLIENT = mockClient;
    }

    /**
     * Call a remote sqlrec function API endpoint (e.g. {@code /api/v1/<name>}) and
     * return the parsed {@link ExecuteData} response.
     *
     * <p>The caller is responsible for inspecting {@link ExecuteData#getData()} and
     * {@link ExecuteData#getMsg()} to decide whether the call succeeded; an empty/null
     * {@code data} typically indicates a remote-side failure described by {@code msg}.
     *
     * @param url        remote API url
     * @param data       input tables keyed by table name, each table is a list of row maps (may be {@code null})
     * @param params     execution variables (may be {@code null})
     * @param metricTags metric tags (may be {@code null})
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteData callFunctionApi(String url,
                                              Map<String, List<Map<String, Object>>> data,
                                              Map<String, String> params,
                                              Map<String, String> metricTags) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is null or empty");
        }

        RequestData requestData = new RequestData();
        requestData.setData(data);
        requestData.setParams(params);
        requestData.setMetricTags(metricTags);
        String bodyJson = JsonUtils.toJson(requestData);

        String responseJson = doPost(url, bodyJson);
        ExecuteData response = JsonUtils.fromJson(responseJson, ExecuteData.class);
        return response != null ? response : new ExecuteData();
    }

    /**
     * Call a remote sqlrec function API endpoint with only the input tables, using
     * empty execution variables and metric tags. Equivalent to
     * {@link #callFunctionApi(String, Map, Map, Map)} with {@code null} params and metricTags.
     *
     * @param url  remote API url
     * @param data input tables keyed by table name, each table is a list of row maps (may be {@code null})
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteData callFunctionApi(String url,
                                              Map<String, List<Map<String, Object>>> data) {
        return callFunctionApi(url, data, null, null);
    }

    /**
     * Call a remote sqlrec SQL API endpoint (e.g. {@code /sql/v1}) and return the parsed
     * {@link ExecuteDataList} response, which carries one {@link ExecuteData} entry per
     * executed SQL statement.
     *
     * @param url        remote API url
     * @param sqls       SQL statements to execute
     * @param params     execution variables (may be {@code null})
     * @param metricTags metric tags (may be {@code null})
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteDataList callSqlApi(String url,
                                             List<String> sqls,
                                             Map<String, String> params,
                                             Map<String, String> metricTags) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is null or empty");
        }

        RequestData requestData = new RequestData();
        requestData.setSqls(sqls);
        requestData.setParams(params);
        requestData.setMetricTags(metricTags);
        String bodyJson = JsonUtils.toJson(requestData);

        String responseJson = doPost(url, bodyJson);
        ExecuteDataList response = JsonUtils.fromJson(responseJson, ExecuteDataList.class);
        return response != null ? response : new ExecuteDataList();
    }

    /**
     * Call a remote sqlrec SQL API endpoint with only the SQL statements, using empty
     * execution variables and metric tags. Equivalent to
     * {@link #callSqlApi(String, List, Map, Map)} with {@code null} params and metricTags.
     *
     * @param url  remote API url
     * @param sqls SQL statements to execute
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteDataList callSqlApi(String url, List<String> sqls) {
        return callSqlApi(url, sqls, null, null);
    }

    private static String doPost(String url, String bodyJson) {
        RequestBody body = RequestBody.create(bodyJson, JSON_MEDIA_TYPE);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Accept", "application/json")
                .build();
        try (Response response = HTTP_CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("HTTP request failed with response code: " + response.code());
            }
            return response.body() != null ? response.body().string() : "";
        } catch (IOException e) {
            throw new RuntimeException("Failed to call remote sqlrec api: " + e.getMessage(), e);
        }
    }
}
