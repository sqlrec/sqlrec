package com.sqlrec.common.rest;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * HTTP mock unit tests for SqlRecApiClient.
 * Injects a mock OkHttpClient to simulate HTTP calls without a real server.
 */
@ExtendWith(MockitoExtension.class)
public class SqlRecApiClientUnitTest {

    @Mock
    private OkHttpClient mockHttpClient;

    @Mock
    private Call mockCall;

    @Mock
    private Response mockResponse;

    @Mock
    private ResponseBody mockBody;

    @BeforeEach
    void setUp() {
        // Inject mock OkHttpClient into the class under test
        SqlRecApiClient.setHttpClientForTest(mockHttpClient);
    }

    @AfterEach
    void tearDown() {
        // Restore a real OkHttpClient to avoid leaking the mock into other tests
        SqlRecApiClient.setHttpClientForTest(new OkHttpClient());
    }

    /**
     * Test callFunctionApi with empty url, should throw IllegalArgumentException.
     */
    @Test
    public void testCallFunctionApiEmptyUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                SqlRecApiClient.callFunctionApi("", new HashMap<>(), null, null));
    }

    /**
     * Test successful function API call: HTTP 200 + valid ExecuteData JSON response.
     */
    @Test
    public void testCallFunctionApiSuccess() throws IOException {
        // Mock the HTTP call chain
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("{\"data\":[],\"status\":\"ok\"}");

        // Build input data
        Map<String, List<Map<String, Object>>> data = new HashMap<>();

        ExecuteData result = SqlRecApiClient.callFunctionApi(
                "http://test", data, null, null);

        // Assert the returned ExecuteData is not null
        assertNotNull(result);
    }

    /**
     * Test callFunctionApi HTTP error: returns 404, should throw RuntimeException.
     */
    @Test
    public void testCallFunctionApiHttpError() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(404);

        assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callFunctionApi("http://test", new HashMap<>(), null, null));
    }

    /**
     * Test callSqlApi with empty url, should throw IllegalArgumentException.
     */
    @Test
    public void testCallSqlApiEmptyUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                SqlRecApiClient.callSqlApi("", Arrays.asList("SELECT 1"), null, null));
    }

    /**
     * Test successful SQL API call: HTTP 200 + valid ExecuteDataList JSON response.
     */
    @Test
    public void testCallSqlApiSuccess() throws IOException {
        // Mock the HTTP call chain
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("{\"dataList\":[],\"status\":\"ok\"}");

        ExecuteDataList result = SqlRecApiClient.callSqlApi(
                "http://test", Arrays.asList("SELECT 1"), null, null);

        // Assert the returned ExecuteDataList is not null
        assertNotNull(result);
    }

    /**
     * Test callFunctionApi with null url, should throw IllegalArgumentException.
     */
    @Test
    public void testCallFunctionApiNullUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                SqlRecApiClient.callFunctionApi(null, new HashMap<>(), null, null));
    }

    /**
     * Test callFunctionApi with malformed JSON response: HTTP 200 but body is not valid JSON,
     * JsonUtils.fromJson throws JsonSyntaxException (a RuntimeException).
     */
    @Test
    public void testCallFunctionApiMalformedJson() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("not valid json");

        assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callFunctionApi("http://test", new HashMap<>(), null, null));
    }

    /**
     * Test callFunctionApi with empty response body: HTTP 200 but body.string() returns "",
     * JsonUtils.fromJson returns null, callFunctionApi returns a new empty ExecuteData.
     */
    @Test
    public void testCallFunctionApiEmptyResponseBody() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("");

        ExecuteData result = SqlRecApiClient.callFunctionApi(
                "http://test", new HashMap<>(), null, null);
        assertNotNull(result);
    }

    /**
     * Test callFunctionApi HTTP 500 error: server returns 500, should throw RuntimeException containing "500".
     */
    @Test
    public void testCallFunctionApiHttp500() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(500);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callFunctionApi("http://test", new HashMap<>(), null, null));
        assertTrue(ex.getMessage().contains("500"));
    }

    /**
     * Test callFunctionApi IOException: execute throws IOException,
     * should be wrapped in RuntimeException with original message.
     */
    @Test
    public void testCallFunctionApiIOException() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenThrow(new IOException("connection refused"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callFunctionApi("http://test", new HashMap<>(), null, null));
        assertTrue(ex.getMessage().contains("connection refused"));
    }

    /**
     * Test callSqlApi with null url, should throw IllegalArgumentException.
     */
    @Test
    public void testCallSqlApiNullUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                SqlRecApiClient.callSqlApi(null, Arrays.asList("SELECT 1"), null, null));
    }

    /**
     * Test callSqlApi with malformed JSON response: HTTP 200 but body is not valid JSON,
     * JsonUtils.fromJson throws JsonSyntaxException (a RuntimeException).
     */
    @Test
    public void testCallSqlApiMalformedJson() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("not json");

        assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callSqlApi("http://test", Arrays.asList("SELECT 1"), null, null));
    }

    /**
     * Test callSqlApi IOException: execute throws IOException,
     * should be wrapped in RuntimeException containing the original message.
     */
    @Test
    public void testCallSqlApiIOException() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenThrow(new IOException("timeout"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callSqlApi("http://test", Arrays.asList("SELECT 1"), null, null));
        assertTrue(ex.getMessage().contains("timeout"));
    }

    /**
     * Test callSqlApi HTTP 500 error: server returns 500, should throw RuntimeException.
     */
    @Test
    public void testCallSqlApiHttp500() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(500);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                SqlRecApiClient.callSqlApi("http://test", Arrays.asList("SELECT 1"), null, null));
        assertTrue(ex.getMessage().contains("500"));
    }
}
