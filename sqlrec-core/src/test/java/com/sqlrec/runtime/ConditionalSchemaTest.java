package com.sqlrec.runtime;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.udf.UdfManager;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConditionalSchemaTest {
    private boolean originalParallelism;

    @BeforeEach void setUp() {
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(ConcurrentCalciteSchema.createRootSchema());
    }

    @AfterEach void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void nullableElseSupportsNullPredicatesAndArithmetic() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            for (boolean condition : List.of(false, true)) {
                SqlFunctionBindable function = compile("nullable_cache",
                        "IF (SELECT " + condition + ") THEN (CACHE TABLE output_table AS SELECT 1 AS id) "
                                + "ELSE (CACHE TABLE output_table AS SELECT CAST(NULL AS INTEGER) AS id)",
                        "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
                assertArrayEquals(new Object[]{!condition, condition ? 2 : null}, execute(function, parallel));
                assertTrue(function.getReturnDataFields().get(1).getType().isNullable());
            }
        }
    }

    @Test
    void timeinUsesBothBranchSchemas() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            for (int budget : List.of(0, 5000)) {
                SqlFunctionBindable function = compile("nullable_timein",
                        "IF TIMEIN (SELECT " + budget + ") THEN (CACHE TABLE output_table AS SELECT 1 AS id) "
                                + "ELSE (CACHE TABLE output_table AS SELECT CAST(NULL AS INTEGER) AS id)",
                        "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
                assertArrayEquals(new Object[]{budget == 0, budget == 0 ? null : 2}, execute(function, parallel));
            }
        }
    }

    @Test
    void noElseIncludesTheRetainedCacheSchema() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            SqlFunctionBindable function = compile("nullable_retained",
                    "CACHE TABLE output_table AS SELECT CAST(NULL AS INTEGER) AS id",
                    "IF (SELECT FALSE) THEN (CACHE TABLE output_table AS SELECT 1 AS id)",
                    "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
            assertArrayEquals(new Object[]{true, null}, execute(function, parallel));
        }
    }

    @Test
    void ifReturnSchemaIsSafeForTheCaller() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            compile("nullable_child",
                    "IF (SELECT FALSE) THEN (RETURN SELECT 1 AS id) "
                            + "ELSE (RETURN SELECT CAST(NULL AS INTEGER) AS id)", "RETURN");
            SqlFunctionBindable parent = compile("nullable_parent",
                    "CACHE TABLE output_table AS CALL nullable_child()",
                    "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
            assertArrayEquals(new Object[]{true, null}, execute(parent, parallel));
        }
    }

    @Test
    void earlyAndFinalReturnsShareAMergedSchema() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            for (boolean condition : List.of(false, true)) {
                compile("early_nullable_child",
                        "IF (SELECT " + condition + ") THEN (RETURN SELECT 1 AS id)",
                        "RETURN SELECT CAST(NULL AS INTEGER) AS id");
                SqlFunctionBindable parent = compile("early_nullable_parent",
                        "CACHE TABLE output_table AS CALL early_nullable_child()",
                        "RETURN SELECT id IS NULL AS missing, id + 1 AS next_id FROM output_table");
                assertArrayEquals(new Object[]{!condition, condition ? 2 : null}, execute(parent, parallel));
            }
        }
    }

    @Test
    void differentStringBoundsAreMerged() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            SqlFunctionBindable function = compile("string_bounds",
                    "IF (SELECT FALSE) THEN (CACHE TABLE output_table AS SELECT 'a' AS text_value) "
                            + "ELSE (CACHE TABLE output_table AS SELECT 'abcdef' AS text_value)",
                    "RETURN SELECT CHAR_LENGTH(text_value) AS text_length FROM output_table");
            assertEquals(6, execute(function, parallel)[0]);
            assertEquals(6, function.getBindableList().getFirst().getCacheTableDataFields().getFirst().getType().getPrecision());
        }
    }

    @Test
    void rejectsDifferentArrayElementsInIfAndTimeinCacheBranches() {
        for (String condition : List.of("(SELECT TRUE)", "TIMEIN (SELECT 0)")) {
            RuntimeException error = assertThrows(RuntimeException.class, () -> compile("array_cache_mismatch",
                    "IF " + condition + " THEN (CACHE TABLE t AS SELECT ARRAY[CAST(1 AS INTEGER)] AS xs) "
                            + "ELSE (CACHE TABLE t AS SELECT ARRAY[CAST(2 AS BIGINT)] AS xs)",
                    "RETURN SELECT xs[1] AS result_value FROM t"));
            assertTrue(error.getMessage().contains("field type not equal"), error.getMessage());
            assertTrue(error.getMessage().contains("INTEGER ARRAY"), error.getMessage());
            assertTrue(error.getMessage().contains("BIGINT ARRAY"), error.getMessage());
        }
    }

    @Test
    void rejectsDifferentArrayElementsAgainstRetainedCache() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> compile("array_retained_mismatch",
                "CACHE TABLE t AS SELECT ARRAY[CAST(1 AS INTEGER)] AS xs",
                "IF (SELECT FALSE) THEN (CACHE TABLE t AS SELECT ARRAY[CAST(2 AS BIGINT)] AS xs)",
                "RETURN SELECT xs[1] AS result_value FROM t"));
        assertTrue(error.getMessage().contains("field type not equal"), error.getMessage());
    }

    @Test
    void rejectsDifferentArrayElementsInReturnSchemas() {
        RuntimeException branchError = assertThrows(RuntimeException.class, () -> compile("array_return_mismatch",
                "IF (SELECT TRUE) THEN (RETURN SELECT ARRAY[CAST(1 AS INTEGER)] AS xs) "
                        + "ELSE (RETURN SELECT ARRAY[CAST(2 AS BIGINT)] AS xs)", "RETURN"));
        assertTrue(branchError.getMessage().contains("field type not equal"), branchError.getMessage());

        RuntimeException earlyReturnError = assertThrows(RuntimeException.class, () -> compile("early_array_mismatch",
                "IF (SELECT TRUE) THEN (RETURN SELECT ARRAY[CAST(1 AS INTEGER)] AS xs)",
                "RETURN SELECT ARRAY[CAST(2 AS BIGINT)] AS xs"));
        assertTrue(earlyReturnError.getMessage().contains("field type not equal"), earlyReturnError.getMessage());
    }

    @Test
    void matchingArrayElementsStillExecuteBothBranches() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            for (boolean condition : List.of(false, true)) {
                SqlFunctionBindable function = compile("matching_array_cache",
                        "IF (SELECT " + condition + ") THEN (CACHE TABLE t AS SELECT ARRAY[1] AS xs) "
                                + "ELSE (CACHE TABLE t AS SELECT ARRAY[2] AS xs)",
                        "RETURN SELECT xs[1] AS result_value FROM t");
                assertArrayEquals(new Object[]{condition ? 1 : 2}, execute(function, parallel));
            }
        }
    }

    @Test
    void explicitArrayCastMakesUdfBranchCompatible() throws Exception {
        CalciteSchema root = ConcurrentCalciteSchema.createRootSchema();
        ScalarFunction randomVec = UdfManager.createScalarFunction("com.sqlrec.udf.scalar.RandomVecFunction");
        root.add("default", new AbstractSchema() {
            @Override
            protected Multimap<String, Function> getFunctionMultimap() {
                return ImmutableMultimap.of("random_vec", randomVec);
            }
        });
        CalciteSchemaFactory.setGlobalSchema(root);

        for (boolean parallel : List.of(false, true)) {
            for (boolean condition : List.of(false, true)) {
                SqlFunctionBindable function = compile("cast_array_cache",
                        "IF (SELECT " + condition + ") THEN (CACHE TABLE t AS SELECT ARRAY[CAST(1 AS FLOAT)] AS xs) "
                                + "ELSE (CACHE TABLE t AS SELECT CAST(random_vec('3') AS FLOAT ARRAY) AS xs)",
                        "RETURN SELECT xs[1] + 1 AS result_value FROM t");
                Number result = assertInstanceOf(Number.class, execute(function, parallel)[0]);
                assertTrue(result.doubleValue() >= 1 && result.doubleValue() <= 2);
                if (condition) {
                    assertEquals(2.0, result.doubleValue());
                }
            }
        }
    }

    private Object[] execute(SqlFunctionBindable function, boolean parallel) {
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable(SqlRecConfigs.IGNORE_UNION_EXCEPTION.getKey(), "false");
        return function.bind(schema, context).toList().getFirst();
    }

    private SqlFunctionBindable compile(String name, String... body) throws Exception {
        List<String> statements = new ArrayList<>();
        statements.add("CREATE SQL FUNCTION " + name);
        statements.addAll(List.of(body));
        return new CompileManager().compileSqlFunction(name, statements);
    }
}
