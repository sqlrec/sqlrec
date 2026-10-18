package com.sqlrec.common.rest;

import com.sqlrec.common.http.JsonHttpTransport;
import com.sqlrec.common.utils.JsonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Credentials;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Lightweight client for calling remote sqlrec REST APIs.
 *
 * <p>External code only needs to depend on the {@code sqlrec-common} module to
 * invoke remote sqlrec endpoints and obtain parsed results, without dealing with
 * HTTP transport or (de)serialization details. Two endpoint families are supported:
 *
 * <ul>
 *   <li>{@link #callFunctionApi} — function endpoints (e.g. {@code /api/v1/<name>}),
 *       request body matches {@link RequestData} ({@code {data, params}}),
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
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteData callFunctionApi(String url,
                                              Map<String, List<Map<String, Object>>> data,
                                              Map<String, String> params) {
        return callFunctionApi(url, data, params, null, null);
    }

    /** Calls an authenticated endpoint; credentials are sent only in the HTTP header. */
    public static ExecuteData callFunctionApi(String url,
                                              Map<String, List<Map<String, Object>>> data,
                                              Map<String, String> params,
                                              String username, String password) {
        RequestData requestData = createRequestData(params);
        requestData.setData(data);
        return postRequest(url, requestData, ExecuteData.class, ExecuteData::new,
                authorization(username, password));
    }

    /**
     * Call a remote sqlrec function API endpoint with only the input tables, using
     * empty execution variables. Equivalent to
     * {@link #callFunctionApi(String, Map, Map)} with {@code null} params.
     *
     * @param url  remote API url
     * @param data input tables keyed by table name, each table is a list of row maps (may be {@code null})
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteData callFunctionApi(String url,
                                              Map<String, List<Map<String, Object>>> data) {
        return callFunctionApi(url, data, null);
    }

    /**
     * Call a remote sqlrec SQL API endpoint (e.g. {@code /sql/v1}) and return the parsed
     * {@link ExecuteDataList} response, which carries one {@link ExecuteData} entry per
     * executed SQL statement.
     *
     * @param url        remote API url
     * @param sqls       SQL statements to execute
     * @param params     execution variables (may be {@code null})
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteDataList callSqlApi(String url,
                                             List<String> sqls,
                                             Map<String, String> params) {
        return callSqlApi(url, sqls, params, null, null);
    }

    /** Calls an authenticated SQL endpoint without adding credentials to execution params. */
    public static ExecuteDataList callSqlApi(String url, List<String> sqls,
                                             Map<String, String> params,
                                             String username, String password) {
        RequestData requestData = createRequestData(params);
        requestData.setSqls(sqls);
        return postRequest(url, requestData, ExecuteDataList.class, ExecuteDataList::new,
                authorization(username, password));
    }

    /**
     * Call a remote sqlrec SQL API endpoint with only the SQL statements, using empty
     * execution variables. Equivalent to
     * {@link #callSqlApi(String, List, Map)} with {@code null} params.
     *
     * @param url  remote API url
     * @param sqls SQL statements to execute
     * @return parsed response; never {@code null}
     * @throws IllegalArgumentException if {@code url} is null or empty
     * @throws RuntimeException         if the HTTP call fails or the response cannot be parsed
     */
    public static ExecuteDataList callSqlApi(String url, List<String> sqls) {
        return callSqlApi(url, sqls, null);
    }

    private static RequestData createRequestData(
            Map<String, String> params
    ) {
        RequestData requestData = new RequestData();
        requestData.setParams(params);
        return requestData;
    }

    private static <T> T postRequest(
            String url,
            RequestData requestData,
            Class<T> responseType,
            Supplier<T> emptyResponse,
            String authorization
    ) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is null or empty");
        }
        T response = JsonUtils.fromJson(
                doPost(url, JsonUtils.toJson(requestData), authorization),
                responseType
        );
        return response != null ? response : emptyResponse.get();
    }

    private static String authorization(String username, String password) {
        if (username == null && password == null) {
            return null;
        }
        if (username == null || password == null) {
            throw new IllegalArgumentException("both username and password are required");
        }
        return Credentials.basic(username, password, StandardCharsets.UTF_8);
    }

    private static String doPost(String url, String bodyJson, String authorization) {
        try {
            return JsonHttpTransport.post(HTTP_CLIENT, url, bodyJson, authorization);
        } catch (IOException e) {
            throw new RuntimeException("Failed to call remote sqlrec api: " + e.getMessage(), e);
        }
    }
}
