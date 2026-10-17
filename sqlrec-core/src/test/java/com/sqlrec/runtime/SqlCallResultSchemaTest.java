package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.schema.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlCallResultSchemaTest {
    private boolean originalParallelism;

    @BeforeEach void setUp() {
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(ConcurrentCalciteSchema.createRootSchema());
        JavaFunctionUtils.setSkipHmsQuery(true);
    }
    @AfterEach void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @Test
    void incompatibleDynamicResultsFailBeforeExecutingTheChild() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            // The child changes a variable if it runs. Schema errors must be detected first.
            for (String result : List.of(
                    "20 AS right_id, 10 AS left_id", // reordered
                    "10 AS left_id",                // missing column
                    "10 AS left_id, 20 AS right_id, 30 AS extra_id",
                    "'bad' AS left_id, 20 AS right_id",
                    "CAST(NULL AS INTEGER) AS left_id, 20 AS right_id")) {
                compile("schema_child", "SET 'child_ran' = 'true'", "RETURN SELECT " + result);
                SqlFunctionBindable parent = compile("schema_parent",
                        "CACHE TABLE template AS SELECT 1 AS left_id, 2 AS right_id",
                        "CACHE TABLE output_table AS CALL GET_OR_DEFAULT('callee', 'schema_child')() LIKE template",
                        "RETURN output_table");
                CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
                ExecuteContextImpl context = context(schema);
                SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> parent.bind(schema, context));
                assertTrue(hasMessage(failure, "return schema does not match LIKE"), failure.toString());
                assertNull(context.getVariable("child_ran"));
            }
        }
    }

    @Test void staticLikeIsAlsoCheckedDuringCompilation() throws Exception {
        compile("static_schema_child", "RETURN SELECT 20 AS right_id, 10 AS left_id");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> compile("static_schema_parent",
                        "CACHE TABLE template AS SELECT 1 AS left_id, 2 AS right_id",
                        "CACHE TABLE output_table AS CALL static_schema_child() LIKE template", "RETURN output_table"));
        assertTrue(failure.getMessage().contains("return schema does not match LIKE"));
    }

    @Test
    void dynamicRecursionWithMatchingLikeStillReportsCircularDependency() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            compile("recursive_schema",
                    "CACHE TABLE template AS SELECT 1 AS id",
                    "CALL GET_OR_DEFAULT('callee', 'recursive_schema')() LIKE template",
                    "RETURN template");
            SqlFunctionBindable caller = compile("recursive_schema_caller", "RETURN CALL recursive_schema()");
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);

            CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> caller.bind(schema, context(schema)));
            assertTrue(hasMessage(failure, "Circular dependency detected: recursive_schema"),
                    () -> failure.toString());
        }
    }

    @Test
    void matchingResultsWorkWithTableFunctionAndPartitionLike() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            compile("schema_echo", "DEFINE INPUT TABLE input_table(id integer)", "RETURN input_table");
            for (String like : List.of("LIKE seed_table", "LIKE FUNCTION 'schema_echo'")) {
                SqlFunctionBindable parent = compile("schema_matching",
                        "CACHE TABLE seed_table AS SELECT 7 AS id",
                        "CACHE TABLE output_table AS CALL GET_OR_DEFAULT('callee', 'schema_echo')(seed_table) "
                                + like + " PARTITION BY seed_table SIZE 1", "RETURN output_table");
                SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
                CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
                assertArrayEquals(new Object[]{7}, parent.bind(schema, context(schema)).toList().getFirst());
            }
        }
    }

    @Test
    void nullableLikeAcceptsNonNullResults() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            compile("nonnull_schema_child", "RETURN SELECT 7 AS id");
            SqlFunctionBindable parent = compile("nullable_schema_parent",
                    "CACHE TABLE template AS SELECT CAST(NULL AS INTEGER) AS id",
                    "CACHE TABLE output_table AS CALL GET_OR_DEFAULT('callee', 'nonnull_schema_child')() LIKE template",
                    "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
            CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
            assertArrayEquals(new Object[]{false, 8}, parent.bind(schema, context(schema)).toList().getFirst());
        }
    }

    @Test
    void dynamicRankerCanMatchALikeFunctionWithAnUnselectedSortKey() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            compile("sorted_ranker", "DEFINE INPUT TABLE items(id integer, score integer)",
                    "RETURN SELECT id FROM items ORDER BY score DESC");
            compile("fallback_ranker", "DEFINE INPUT TABLE items(id integer, score integer)",
                    "RETURN SELECT id FROM items");
            SqlFunctionBindable parent = compile("ranker_parent",
                    "CACHE TABLE seed_table AS SELECT 1 AS id, 10 AS score UNION ALL SELECT 2, 20",
                    "CACHE TABLE output_table AS CALL GET('ranker')(seed_table) LIKE FUNCTION 'sorted_ranker'",
                    "RETURN output_table");
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);

            for (String ranker : List.of("sorted_ranker", "fallback_ranker")) {
                CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
                ExecuteContextImpl context = context(schema);
                context.setVariable("ranker", ranker);
                List<Object[]> rows = parent.bind(schema, context).toList();
                assertEquals(1, parent.getReturnDataFields().size());
                assertEquals(2, rows.size());
                boolean sorted = ranker.equals("sorted_ranker");
                assertArrayEquals(new Object[]{sorted ? 2 : 1}, rows.get(0));
                assertArrayEquals(new Object[]{sorted ? 1 : 2}, rows.get(1));
            }
        }
    }

    private ExecuteContextImpl context(CalciteSchema schema) {
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable(SqlRecConfigs.IGNORE_UNION_EXCEPTION.getKey(), "false");
        return context;
    }
    private boolean hasMessage(Throwable failure, String message) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(message)) return true;
        }
        return false;
    }
    private SqlFunctionBindable compile(String name, String... body) throws Exception {
        List<String> statements = new ArrayList<>();
        statements.add("CREATE SQL FUNCTION " + name);
        statements.addAll(List.of(body));
        return new CompileManager().compileSqlFunction(name, statements);
    }
}
