package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.JsonUtils;
import com.google.gson.JsonParseException;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.udf.table.CallSqlRecApiFunction;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RestExecutorTest {
    @Test
    void functionApiPreservesAllNullColumnsForTheCallersLikeSchema() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            var fields = List.of(DataTypeUtils.getRelDataTypeField("score", 0, "DOUBLE"),
                    DataTypeUtils.getRelDataTypeField("id", 1, "BIGINT"));
            var attributes = DataTypeUtils.getRelDataTypeField("attributes", 2, "ANY");
            var nested = Collections.singletonMap("missing", null);
            when(fixture.proxy.getReturnDataFields()).thenReturn(List.of(fields.get(0), fields.get(1), attributes));
            when(fixture.proxy.bind(any(), any())).thenReturn(Linq4j.asEnumerable(
                    List.of(new Object[]{null, 1L, nested}, new Object[]{null, 2L, nested})));

            String json = postFunctionApi("{}", HttpResponseStatus.OK);
            Map<String, Object> body = JsonUtils.parseJsonToMap(json);
            assertFalse(body.containsKey("columns"));
            assertFalse(body.containsKey("msg"));
            var response = JsonUtils.fromJson(json, ExecuteData.class);
            assertEquals(2, response.getData().size());
            for (int i = 0; i < response.getData().size(); i++) {
                var row = response.getData().get(i);
                assertTrue(row.containsKey("score"));
                assertNull(row.get("score"));
                assertEquals(i + 1L, row.get("id"));
                assertEquals(Map.of(), row.get("attributes"));
            }

            var call = new CallSqlRecApiFunction((url, data, params) -> response);
            var input = new CacheTable("input", Linq4j.emptyEnumerable(), fields);
            var output = call.evaluate(new ExecuteContextImpl(fixture.schema), "http://unused.invalid", input);
            assertEquals(0, DataTypeUtils.findFieldIndex(output.getDataFields(), "score"));
            assertEquals(SqlTypeName.NULL, output.getDataFields().getFirst().getType().getSqlTypeName());

            var rows = RowTransformUtils.adaptRowsToSchema(output.scan(null), fields, output.getDataFields());
            assertArrayEquals(new Object[]{null, 1L}, rows.get(0));
            assertArrayEquals(new Object[]{null, 2L}, rows.get(1));

            var missing = List.of(DataTypeUtils.getRelDataTypeField("missing", 0, "DOUBLE"));
            assertTrue(assertThrows(RuntimeException.class, () -> RowTransformUtils.adaptRowsToSchema(
                    output.scan(null), missing, output.getDataFields())).getMessage().contains("desired field not found"));
        }
    }

    private static String postFunctionApi(String body, HttpResponseStatus expectedStatus) {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/v1/example",
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(expectedStatus, response.status());
                return response.content().toString(CharsetUtil.UTF_8);
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void excludesFunctionResultParamsWithoutChangingExecutionVariables() throws Exception {
        boolean previous = SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.getDefaultValue();
        SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.setDefaultValue(false);
        try (FunctionFixture fixture = new FunctionFixture()) {
            when(fixture.proxy.bind(any(), any())).thenAnswer(call -> {
                ExecuteContext context = call.getArgument(1);
                assertEquals("value", context.getVariable("key"));
                assertEquals("true", context.getVariable("EXECUTE_RESULT_INCLUDE_PARAMS"));
                return Linq4j.asEnumerable(Collections.singletonList(new Object[]{"done"}));
            });
            ExecuteData result = RestFunctionExecutor.execute("example",
                    "{\"params\":{\"key\":\"value\",\"EXECUTE_RESULT_INCLUDE_PARAMS\":\"true\"}}");
            assertEquals(List.of(Map.of("value", "done")), result.getData());
            assertNull(result.getParams());
            assertFalse(JsonUtils.parseJsonToMap(JsonUtils.toJson(result)).containsKey("params"));
        } finally {
            SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.setDefaultValue(previous);
        }
    }

    @Test
    void excludesSqlResultParamsWithoutChangingExecutionInputsOrRows() throws Exception {
        boolean previous = SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.getDefaultValue();
        SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.setDefaultValue(false);
        try (var executors = mockConstruction(SqlExecutor.class, (executor, construction) -> {
            when(executor.executeSql("ok")).thenReturn(new CacheTable("result",
                    Linq4j.asEnumerable(Collections.singletonList(new Object[]{"done"})),
                    DataTypeUtils.getStringTypeField("value")));
            ExecuteContext context = mock(ExecuteContext.class);
            when(context.getVariables()).thenReturn(Map.of("key", "value"));
            when(executor.getExecuteContext()).thenReturn(context);
        })) {
            ExecuteData result = RestSqlExecutor.execute(
                    "{\"sqls\":[\"ok\"],\"params\":{\"key\":\"value\"}}").getData().get(0);
            verify(executors.constructed().get(0)).setExecuteParams(Map.of("key", "value"));
            assertEquals(List.of(Map.of("value", "done")), result.getData());
            assertNull(result.getParams());
            assertFalse(JsonUtils.parseJsonToMap(JsonUtils.toJson(result)).containsKey("params"));
        } finally {
            SqlRecConfigs.EXECUTE_RESULT_INCLUDE_PARAMS.setDefaultValue(previous);
        }
    }

    @Test
    void keepsFunctionValidationOrderAndMessages() {
        assertEquals("API name is required", assertThrows(IllegalArgumentException.class,
                () -> RestFunctionExecutor.execute(" ", "{bad")).getMessage());
        for (String body : new String[]{null, "", " \t"}) {
            assertEquals("request body is required; expected a JSON object",
                    assertThrows(IllegalArgumentException.class,
                            () -> RestFunctionExecutor.execute("example", body)).getMessage());
        }
        assertEquals("request body must be a JSON object", assertThrows(IllegalArgumentException.class,
                () -> RestFunctionExecutor.execute("example", "null")).getMessage());
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RestFunctionExecutor.execute("example", "{bad"));
        assertTrue(error.getMessage().startsWith("invalid JSON request body: "));
        assertInstanceOf(JsonParseException.class, error.getCause());
    }

    @Test
    void keepsSqlValidationMessages() {
        for (String body : new String[]{null, "", " \t"}) {
            assertEquals("request body is required; expected JSON with a non-empty sqls array",
                    assertThrows(IllegalArgumentException.class,
                            () -> RestSqlExecutor.execute(body)).getMessage());
        }
        assertEquals("request body must be a JSON object with a non-empty sqls array",
                assertThrows(IllegalArgumentException.class, () -> RestSqlExecutor.execute("null")).getMessage());
        for (String body : new String[]{"{}", "{\"sqls\":null}", "{\"sqls\":[]}"}) {
            assertEquals("sqls must be a non-empty array",
                    assertThrows(IllegalArgumentException.class, () -> RestSqlExecutor.execute(body)).getMessage());
        }
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RestSqlExecutor.execute("{bad"));
        assertTrue(error.getMessage().startsWith("invalid JSON request body: "));
        assertInstanceOf(JsonParseException.class, error.getCause());
    }

    @Test
    void tagsSqlWithTheFirstWordAfterComments() throws Exception {
        String sql = "/* query */ -- note\nselect 1";
        var composite = MetricsUtils.getCompositeMeterRegistry();
        var registry = new SimpleMeterRegistry();
        composite.add(registry);
        try (var executors = mockConstruction(SqlExecutor.class, (executor, construction) -> {
            ExecuteContext context = mock(ExecuteContext.class);
            when(executor.getExecuteContext()).thenReturn(context);
            when(context.getVariables()).thenReturn(Map.of());
            when(executor.executeSql(sql)).thenReturn(new CacheTable("result",
                    Linq4j.emptyEnumerable(), DataTypeUtils.getStringTypeField("value")));
        })) {
            RestSqlExecutor.execute(JsonUtils.toJson(Map.of("sqls", List.of(sql))));

            verify(executors.constructed().getFirst()).executeSql(sql);
            assertEquals(1, registry.get(Consts.METRICS_SQL_EXECUTION_DURATION)
                    .tags("statement_type", "SELECT", "outcome", "success").timer().count());
        } finally {
            composite.remove(registry);
            registry.close();
        }
    }

    @Test
    void initializesInputTablesAndContextBeforeBindingAndCopiesParamsBeforeReadingRows() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            when(fixture.function.getInputTables()).thenReturn(
                    List.of(Map.entry("input", DataTypeUtils.getStringTypeField("value"))));
            AtomicReference<ExecuteContext> context = new AtomicReference<>();
            var rows = Linq4j.singletonEnumerable(new Object[]{"result"}).select(row -> {
                context.get().setVariable("value", "after");
                return row;
            });
            when(fixture.proxy.bind(same(fixture.schema), any())).thenAnswer(call -> {
                ExecuteContext execution = call.getArgument(1);
                context.set(execution);
                assertEquals("before", execution.getVariable("value"));
                assertEquals("test", execution.getVariable("source"));
                assertArrayEquals(new Object[]{"input value"}, ((CacheTable) fixture.schema.plus()
                        .getTable("input")).scan(null).toList().get(0));
                return rows;
            });

            ExecuteData result = RestFunctionExecutor.execute(" Example ",
                    "{\"data\":{\"input\":[{\"value\":\"input value\"}]},"
                            + "\"params\":{\"value\":\"before\",\"source\":\"test\"}}");

            assertEquals(List.of(Map.of("value", "result")), result.getData());
            assertEquals("before", result.getParams().get("value"));
            assertEquals("after", context.get().getVariable("value"));
            assertFalse(context.get().isCancelled());
            assertNull(result.getMsg());
            fixture.apis.verify(() -> SqlApiCache.get("example"));
        }
    }

    @Test
    void returnsVariablesAndMessageWhenFunctionHasNoResult() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            ExecuteData result = RestFunctionExecutor.execute("EXAMPLE", "{\"params\":{\"key\":\"value\"}}");
            assertEquals("API 'example' returned no result", result.getMsg());
            assertEquals("value", result.getParams().get("key"));
            assertNull(result.getData());
        }
    }

    @Test
    @SilenceLoggers(RestFunctionExecutor.class)
    void executionFailureCancelsContextAndPreservesCause() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            AtomicReference<ExecuteContext> context = new AtomicReference<>();
            IllegalStateException failure = new IllegalStateException(" ");
            when(fixture.proxy.bind(any(), any())).thenAnswer(call -> {
                context.set(call.getArgument(1));
                throw failure;
            });
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> RestFunctionExecutor.execute("EXAMPLE", "{}"));
            assertEquals("failed to execute API 'example': function execution failed", error.getMessage());
            assertSame(failure, error.getCause());
            assertTrue(context.get().isCancelled());
        }
    }

    @Test
    void inputValidationFailsBeforeContextCreationAndExecutionPreparation() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture();
             var contexts = mockConstruction(ExecuteContextImpl.class)) {
            when(fixture.function.getInputTables()).thenReturn(
                    List.of(Map.entry("input", DataTypeUtils.getStringTypeField("value"))));
            assertEquals("data is required for input table 'input'", assertThrows(IllegalArgumentException.class,
                    () -> RestFunctionExecutor.execute("example", "{}")).getMessage());
            assertEquals("input table 'input' is missing from data", assertThrows(IllegalArgumentException.class,
                    () -> RestFunctionExecutor.execute("example", "{\"data\":{}}")).getMessage());
            assertTrue(contexts.constructed().isEmpty());
            fixture.preparation.verifyNoInteractions();
        }
    }

    @Test
    void nullInputTableReturnsBadRequestAndEmptyArrayIsAccepted() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            when(fixture.function.getInputTables()).thenReturn(
                    List.of(Map.entry("input", DataTypeUtils.getStringTypeField("value"))));

            String error = postFunctionApi("{\"data\":{\"input\":null}}", HttpResponseStatus.BAD_REQUEST);
            assertEquals("input table 'input' must be an array", JsonUtils.fromJson(error, ExecuteData.class).getMsg());
            fixture.preparation.verifyNoInteractions();

            postFunctionApi("{\"data\":{\"input\":[]}}", HttpResponseStatus.OK);
            assertTrue(((CacheTable) fixture.schema.plus().getTable("input")).scan(null).toList().isEmpty());
        }
    }

    @Test
    void missingFunctionFailsBeforeSchemaCreation() throws Exception {
        try (var apis = mockStatic(SqlApiCache.class);
             var schemas = mockStatic(CalciteSchemaFactory.class);
             var compilers = mockConstruction(CompileManager.class)) {
            SqlApi api = mock(SqlApi.class);
            when(api.getFunctionName()).thenReturn("missing");
            apis.when(() -> SqlApiCache.get("example")).thenReturn(api);
            assertEquals("function 'missing' configured for API 'example' was not found",
                    assertThrows(IllegalStateException.class,
                            () -> RestFunctionExecutor.execute("EXAMPLE", "{}")).getMessage());
            schemas.verifyNoInteractions();
        }
    }

    @Test
    void sqlBatchSkipsBlankStatementsAndContinuesAfterFailure() throws Exception {
        try (var executors = mockConstruction(SqlExecutor.class, (executor, context) -> {
            when(executor.executeSql("bad")).thenThrow(new IllegalArgumentException("bad SQL"));
            when(executor.executeSql("ok")).thenReturn(new CacheTable("result",
                    Linq4j.asEnumerable(Collections.singletonList(new Object[]{"done"})),
                    DataTypeUtils.getStringTypeField("value")));
            ExecuteContext execution = mock(ExecuteContext.class);
            when(execution.getVariables()).thenReturn(Map.of("key", "value"));
            when(executor.getExecuteContext()).thenReturn(execution);
        })) {
            List<ExecuteData> results = RestSqlExecutor.execute(
                    "{\"sqls\":[null,\" \",\"bad\",\"ok\"],\"params\":{\"key\":\"value\"}}").getData();
            assertEquals(4, results.size());
            assertEquals("sqls[0] is null or empty; skipped execution", results.get(0).getMsg());
            assertEquals("sqls[1] is null or empty; skipped execution", results.get(1).getMsg());
            assertEquals("sqls[2] failed: bad SQL", results.get(2).getMsg());
            assertEquals(List.of(Map.of("value", "done")), results.get(3).getData());
            assertEquals(Map.of("key", "value"), results.get(3).getParams());
            SqlExecutor executor = executors.constructed().get(0);
            var order = inOrder(executor);
            order.verify(executor).setExecuteParams(Map.of("key", "value"));
            order.verify(executor).executeSql("bad");
            order.verify(executor).executeSql("ok");
        }
    }

    private static final class FunctionFixture implements AutoCloseable {
        private final SqlFunctionBindable function = mock(SqlFunctionBindable.class);
        private final BindableInterface proxy = mock(BindableInterface.class);
        private final CalciteSchema schema = CalciteSchema.createRootSchema(false);
        private final MockedStatic<SqlApiCache> apis = mockStatic(SqlApiCache.class);
        private final MockedStatic<CalciteSchemaFactory> schemas = mockStatic(CalciteSchemaFactory.class);
        private final MockedStatic<CompileManager> preparation = mockStatic(CompileManager.class);
        private final MockedConstruction<CompileManager> compilers;

        private FunctionFixture() throws Exception {
            SqlApi api = mock(SqlApi.class);
            when(api.getFunctionName()).thenReturn("function");
            apis.when(() -> SqlApiCache.get("example")).thenReturn(api);
            schemas.when(CalciteSchemaFactory::createCalciteSchema).thenReturn(schema);
            preparation.when(() -> CompileManager.prepareSqlFunctionForExecution(function)).thenReturn(proxy);
            when(function.getInputTables()).thenReturn(List.of());
            when(proxy.getReturnDataFields()).thenReturn(DataTypeUtils.getStringTypeField("value"));
            compilers = mockConstruction(CompileManager.class, (compiler, context) ->
                    when(compiler.getSqlFunction("function")).thenReturn(function));
        }

        @Override
        public void close() {
            compilers.close();
            preparation.close();
            schemas.close();
            apis.close();
        }
    }
}
