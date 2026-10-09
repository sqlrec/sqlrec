package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.SilenceLoggers;
import com.google.gson.JsonParseException;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
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
    void initializesInputTablesAndContextBeforeBindingAndCopiesParamsBeforeReadingRows() throws Exception {
        try (FunctionFixture fixture = new FunctionFixture()) {
            when(fixture.function.getInputTables()).thenReturn(
                    List.of(Map.entry("input", DataTypeUtils.getStringTypeField("value"))));
            AtomicReference<ExecuteContext> context = new AtomicReference<>();
            Enumerable<Object[]> rows = mock(Enumerable.class);
            when(fixture.proxy.bind(same(fixture.schema), any())).thenAnswer(call -> {
                ExecuteContext execution = call.getArgument(1);
                context.set(execution);
                assertEquals("before", execution.getVariable("value"));
                assertEquals("test", execution.getMetricsTags().get("source"));
                assertArrayEquals(new Object[]{"input value"}, ((CacheTable) fixture.schema.plus()
                        .getTable("input")).scan(null).toList().get(0));
                return rows;
            });
            when(rows.toList()).thenAnswer(call -> {
                context.get().setVariable("value", "after");
                return Collections.singletonList(new Object[]{"result"});
            });

            ExecuteData result = RestFunctionExecutor.execute(" Example ",
                    "{\"data\":{\"input\":[{\"value\":\"input value\"}]},"
                            + "\"params\":{\"value\":\"before\"},\"metricTags\":{\"source\":\"test\"}}");

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
