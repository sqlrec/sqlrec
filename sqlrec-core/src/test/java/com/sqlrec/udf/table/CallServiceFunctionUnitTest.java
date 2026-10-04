package com.sqlrec.udf.table;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * HTTP mock unit tests for CallServiceFunction.
 * Injects a mock OkHttpClient to simulate HTTP calls without a real server.
 */
@ExtendWith(MockitoExtension.class)
public class CallServiceFunctionUnitTest {

    @Mock
    private OkHttpClient mockHttpClient;

    @Mock
    private Call mockCall;

    @Mock
    private Response mockResponse;

    @Mock
    private ResponseBody mockBody;

    /**
     * Test successful prediction service call: HTTP 200 + valid JSON response.
     */
    @Test
    public void testCallPredictionServiceSuccess() throws IOException {
        // Mock the HTTP call chain: httpClient -> call -> response -> body
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("{\"key\":\"value\",\"score\":0.95}");

        Map<String, Object> result = CallServiceFunction.callPredictionService(mockHttpClient,
                "http://test", "{\"input\":[1,2]}");

        // Assert the returned Map contains expected key-value pairs
        assertEquals("value", result.get("key"));
        assertEquals(0.95, result.get("score"));

        // Verify httpClient.newCall was called exactly once
        verify(mockHttpClient, times(1)).newCall(any(Request.class));
    }

    /**
     * Test HTTP error: server returns 500, should throw RuntimeException containing the status code.
     */
    @Test
    public void testCallPredictionServiceHttpError() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(500);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));

        assertTrue(ex.getMessage().contains("500"));
    }

    /**
     * Test IOException: execute throws IOException, should be wrapped in RuntimeException with original message.
     */
    @Test
    public void testCallPredictionServiceThrowsIOException() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenThrow(new IOException("timeout"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));

        assertTrue(ex.getMessage().contains("timeout"));
    }

    /**
     * Test null response body: HTTP 200 but body is null, parsing empty string returns null.
     */
    @Test
    public void testCallPredictionServiceNullBody() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(null);

        // When body is null, responseBody becomes empty string; Gson returns null for empty JSON
        Map<String, Object> result = CallServiceFunction.callPredictionService(mockHttpClient,
                "http://test", "{\"input\":[1,2]}");
        assertNull(result);
    }

    /**
     * Test malformed JSON response: HTTP 200 but body is not valid JSON,
     * Gson throws JsonSyntaxException (a RuntimeException) which is not caught by
     * the IOException catch block and propagates directly.
     */
    @Test
    public void testCallPredictionServiceMalformedJson() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("not valid json");

        assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));
    }

    /**
     * Test empty response body: HTTP 200 but body.string() returns empty string,
     * Gson returns null for empty JSON input.
     */
    @Test
    public void testCallPredictionServiceEmptyResponseBody() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("");

        Map<String, Object> result = CallServiceFunction.callPredictionService(mockHttpClient,
                "http://test", "{\"input\":[1,2]}");
        assertNull(result);
    }

    /**
     * Test HTTP 400 error: server returns 400, should throw RuntimeException containing "400".
     */
    @Test
    public void testCallPredictionServiceHttp400() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(400);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));
        assertTrue(ex.getMessage().contains("400"));
    }

    /**
     * Test HTTP 503 error: server returns 503, should throw RuntimeException containing "503".
     */
    @Test
    public void testCallPredictionServiceHttp503() throws IOException {
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(false);
        when(mockResponse.code()).thenReturn(503);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));
        assertTrue(ex.getMessage().contains("503"));
    }

    /**
     * Test connection failure: httpClient.newCall throws RuntimeException (not IOException),
     * should propagate directly without being wrapped by the IOException catch block.
     */
    @Test
    public void testCallPredictionServiceConnectTimeout() {
        when(mockHttpClient.newCall(any(Request.class)))
                .thenThrow(new RuntimeException("Failed to connect"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                CallServiceFunction.callPredictionService(mockHttpClient, "http://test", "{\"input\":[1,2]}"));
        assertTrue(ex.getMessage().contains("Failed to connect"));
    }

    @Test
    public void testQueryValueOverload() throws IOException {
        ReadonlyContext context = org.mockito.Mockito.mock(ReadonlyContext.class);
        ModelController controller = org.mockito.Mockito.mock(ModelController.class);
        ModelConf modelConf = new ModelConf();
        modelConf.setInputFields(Arrays.asList(
                new FieldSchema("region", "STRING"),
                new FieldSchema("item_id", "BIGINT"),
                new FieldSchema("user_id", "BIGINT"),
                new FieldSchema("missing", "STRING")));

        ServiceConf serviceConf = new ServiceConf();
        serviceConf.setUrl("http://test");
        serviceConf.setModelConfig(modelConf);

        when(context.getServiceConfig("rec_service")).thenReturn(serviceConf);
        when(context.getModelController(modelConf)).thenReturn(controller);
        when(controller.getOutputFields(modelConf))
                .thenReturn(Collections.singletonList(new FieldSchema("score", "DOUBLE")));
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("{\"score\":[0.9,0.8]}");

        CacheTable user = new CacheTable(
                "user",
                Linq4j.asEnumerable(Collections.singletonList(new Object[]{1001L, "cn"})),
                Arrays.asList(field("USER_ID", SqlTypeName.BIGINT),
                        new RelDataTypeFieldImpl("REGION", 1,
                                new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR))));
        CacheTable item = new CacheTable(
                "item",
                Linq4j.asEnumerable(Arrays.asList(new Object[]{11L}, new Object[]{12L})),
                Collections.singletonList(field("item_id", SqlTypeName.BIGINT)));

        CacheTable result = new CallServiceFunction(mockHttpClient).evaluate(context, "rec_service", user, item);
        List<Object[]> rows = result.scan(null).toList();

        assertEquals(2, rows.size());
        assertEquals(11L, rows.get(0)[0]);
        assertEquals(0.9, rows.get(0)[1]);
        assertEquals(12L, rows.get(1)[0]);
        assertEquals(0.8, rows.get(1)[1]);
        assertEquals("score", result.getDataFields().get(1).getName());

        ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
        verify(mockHttpClient).newCall(request.capture());
        Buffer body = new Buffer();
        request.getValue().body().writeTo(body);
        assertEquals("{\"region\":[\"cn\"],\"user_id\":[1001],\"item_id\":[11,12]}", body.readUtf8());
    }

    private static RelDataTypeField field(String name, SqlTypeName typeName) {
        return new RelDataTypeFieldImpl(
                name, 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, typeName));
    }

    @Test
    public void testMultiTargetSchemaWithMissingOnlineLabelsInBothRequestLayouts() throws IOException {
        ReadonlyContext context = org.mockito.Mockito.mock(ReadonlyContext.class);
        ModelConf model = new ModelConf();
        model.setInputFields(Arrays.asList(new FieldSchema("user_id", "BIGINT"), new FieldSchema("item_id", "BIGINT"),
                new FieldSchema("click", "INT"), new FieldSchema("watch", "DOUBLE")));
        model.setParams(Map.of("model", "tzrec.mmoe", "label_columns", "click,watch", "task.watch.type", "regression"));
        ServiceConf service = new ServiceConf();
        service.setUrl("http://test"); service.setModelConfig(model);
        when(context.getServiceConfig("multi_rank")).thenReturn(service);
        when(context.getModelController(model)).thenReturn(new com.sqlrec.model.tzrec.MMoEModel());
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);
        when(mockResponse.isSuccessful()).thenReturn(true);
        when(mockResponse.body()).thenReturn(mockBody);
        when(mockBody.string()).thenReturn("{\"y_watch\":[4.0,2.0],\"probs_click\":[0.8,0.3]}");
        CacheTable rows = new CacheTable("features", Linq4j.asEnumerable(Arrays.asList(new Object[]{7L, 11L}, new Object[]{7L, 12L})),
                Arrays.asList(field("user_id", SqlTypeName.BIGINT), new RelDataTypeFieldImpl("item_id", 1,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT))));
        CacheTable user = new CacheTable("user", Linq4j.asEnumerable(Collections.singletonList(new Object[]{7L})),
                Collections.singletonList(field("user_id", SqlTypeName.BIGINT)));
        CacheTable items = new CacheTable("items", Linq4j.asEnumerable(Arrays.asList(new Object[]{11L}, new Object[]{12L})),
                Collections.singletonList(field("item_id", SqlTypeName.BIGINT)));
        CallServiceFunction function = new CallServiceFunction(mockHttpClient);
        for (CacheTable result : Arrays.asList(function.evaluate(context, "multi_rank", rows), function.evaluate(context, "multi_rank", user, items))) {
            List<Object[]> output = result.scan(null).toList();
            int offset = output.get(0).length - 2;
            assertEquals(2, output.size());
            assertEquals("probs_click", result.getDataFields().get(offset).getName());
            assertEquals("y_watch", result.getDataFields().get(offset + 1).getName());
            assertEquals(0.8, output.get(0)[offset]); assertEquals(4.0, output.get(0)[offset + 1]);
            assertEquals(0.3, output.get(1)[offset]); assertEquals(2.0, output.get(1)[offset + 1]);
        }
        ArgumentCaptor<Request> requests = ArgumentCaptor.forClass(Request.class);
        verify(mockHttpClient, org.mockito.Mockito.times(2)).newCall(requests.capture());
        List<String> bodies = new java.util.ArrayList<>();
        for (Request request : requests.getAllValues()) {
            Buffer body = new Buffer(); request.body().writeTo(body); bodies.add(body.readUtf8());
        }
        assertEquals("[{\"user_id\":7,\"item_id\":11},{\"user_id\":7,\"item_id\":12}]", bodies.get(0));
        assertEquals("{\"user_id\":[7],\"item_id\":[11,12]}", bodies.get(1));
    }
}
