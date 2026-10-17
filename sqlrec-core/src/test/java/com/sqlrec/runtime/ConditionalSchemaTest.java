package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import org.apache.calcite.jdbc.CalciteSchema;
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
