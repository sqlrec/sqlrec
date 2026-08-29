package com.sqlrec.compiler;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlFunctionReturnTest {

    private CalciteSchema schema;
    private boolean originalParallelism;

    @BeforeEach
    void setUp() {
        CompileManager.invalidateCache();
        schema = CalciteSchema.createRootSchema(false);
        CalciteSchemaFactory.setGlobalSchema(schema);
        JavaFunctionUtils.setSkipHmsQuery(true);
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
    }

    @AfterEach
    void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        CompileManager.invalidateCache();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @Test
    void executesEmptyReturn() throws Exception {
        SqlFunctionBindable function = compile("return_empty", "RETURN");

        Enumerable<Object[]> result = function.bind(schema, new ExecuteContextImpl());

        assertNull(result);
        assertNull(function.getReturnDataFields());
    }

    @Test
    void executesReturnSelect() throws Exception {
        SqlFunctionBindable function = compile(
                "return_select",
                "RETURN SELECT 1 AS id, 'ok' AS name"
        );

        List<Object[]> rows = function.bind(schema, new ExecuteContextImpl()).toList();

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("ok", rows.get(0)[1]);
        assertEquals(List.of("id", "name"), function.getReturnDataFields().stream()
                .map(field -> field.getName().toLowerCase())
                .toList());
    }

    @Test
    void executesReturnTable() throws Exception {
        SqlFunctionBindable function = compile(
                "return_table",
                "CACHE TABLE result_table AS SELECT 3 AS id UNION ALL SELECT 4 AS id",
                "RETURN result_table"
        );

        List<Integer> values = function.bind(schema, new ExecuteContextImpl())
                .select(row -> (Integer) row[0])
                .toList();

        assertEquals(List.of(3, 4), values);
    }

    @Test
    void executesReturnCallAndTracksSqlFunctionDependency() throws Exception {
        compile("return_child", "RETURN SELECT 7 AS id");
        SqlFunctionBindable parent = compile(
                "return_parent",
                "RETURN CALL return_child()"
        );

        assertEquals(7, parent.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
        assertEquals(java.util.Set.of("return_child"), parent.getDependencySqlFunctions());
        assertEquals("return_child", parent.getAllDependSqlFunctionMap().get("return_child"));
    }

    @Test
    void nestedFunctionReturnDoesNotTerminateCallingFunction() throws Exception {
        compile("isolation_child", "RETURN SELECT 1 AS id");
        SqlFunctionBindable parent = compile(
                "isolation_parent",
                "CALL isolation_child()",
                "RETURN SELECT 2 AS id"
        );

        assertEquals(2, parent.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
    }

    @Test
    void returnSelectCanReadAFunctionInput() throws Exception {
        new CompileManager().compileSqlFunction("return_input", List.of(
                "CREATE SQL FUNCTION return_input",
                "DEFINE INPUT TABLE input1(id integer)",
                "RETURN SELECT id + 1 AS id FROM input1"
        ));

        new SqlTestCase(
                "CACHE TABLE input_data AS SELECT 5 AS id",
                List.<Object[]>of(new Object[]{"input_data", 1L})
        ).test(schema);
        new SqlTestCase(
                "CALL return_input(input_data)",
                List.<Object[]>of(new Object[]{6})
        ).test(schema);
    }

    @Test
    void earlyIfReturnStopsLaterStatementsInSerialAndParallelModes() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
            String name = parallel ? "early_parallel" : "early_serial";
            SqlFunctionBindable function = compile(
                    name,
                    "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id)",
                    "CACHE TABLE should_not_exist AS SELECT 99 AS id",
                    "RETURN SELECT 2 AS id"
            );

            assertEquals(1, function.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
            assertNull(schema.getTable("should_not_exist", false));
        }
    }

    @Test
    void falseIfWithoutElseContinuesToTopLevelReturn() throws Exception {
        SqlFunctionBindable function = compile(
                "return_fallthrough",
                "IF (SELECT FALSE) THEN (RETURN SELECT 1 AS id)",
                "RETURN SELECT 2 AS id"
        );

        assertEquals(2, function.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
    }

    @Test
    void ifWithTwoReturnBranchesExecutesSelectedBranch() throws Exception {
        SqlFunctionBindable trueFunction = compile(
                "return_if_true",
                "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id) ELSE (RETURN SELECT 2 AS id)",
                "RETURN SELECT 3 AS id"
        );
        SqlFunctionBindable falseFunction = compile(
                "return_if_false",
                "IF (SELECT FALSE) THEN (RETURN SELECT 1 AS id) ELSE (RETURN SELECT 2 AS id)",
                "RETURN SELECT 3 AS id"
        );

        assertEquals(1, trueFunction.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
        assertEquals(2, falseFunction.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
    }

    @Test
    void compiledTimeinReturnCommitsTheSelectedBranch() throws Exception {
        SqlFunctionBindable function = compile(
                "return_timein",
                "IF TIMEIN (SELECT 1000) THEN (RETURN SELECT 1 AS id) "
                        + "ELSE (RETURN SELECT 2 AS id)",
                "RETURN SELECT 3 AS id"
        );

        assertEquals(1, function.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
    }

    @Test
    void returnCallTracksTransitiveSqlFunctionDependencies() throws Exception {
        compile("dependency_leaf", "RETURN SELECT 1 AS id");
        compile("dependency_middle", "RETURN CALL dependency_leaf()");
        SqlFunctionBindable root = compile("dependency_root", "RETURN CALL dependency_middle()");

        assertEquals(java.util.Set.of("dependency_middle"), root.getDependencySqlFunctions());
        assertEquals("dependency_middle->dependency_leaf",
                root.getAllDependSqlFunctionMap().get("dependency_leaf"));
    }

    @Test
    void ifReturnTracksDependenciesFromBothBranches() throws Exception {
        compile("dependency_left", "RETURN SELECT 1 AS id");
        compile("dependency_right", "RETURN SELECT 2 AS id");
        SqlFunctionBindable root = compile(
                "dependency_if_root",
                "IF (SELECT TRUE) THEN (RETURN CALL dependency_left()) "
                        + "ELSE (RETURN CALL dependency_right())",
                "RETURN SELECT 3 AS id"
        );

        assertEquals(
                java.util.Set.of("dependency_left", "dependency_right"),
                root.getDependencySqlFunctions()
        );
    }

    @Test
    void topLevelReturnRemainsRequiredAsFunctionTerminator() {
        RuntimeException error = assertThrows(RuntimeException.class, () ->
                new CompileManager().compileSqlFunction("missing_terminator", List.of(
                        "CREATE SQL FUNCTION missing_terminator",
                        "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id)"
                ))
        );

        assertTrue(error.getMessage().contains("function define end without return"));
    }

    @Test
    void statementsAfterTopLevelReturnAreRejected() {
        Exception error = assertThrows(Exception.class, () -> new FunctionCompiler(schema, new CompileManager())
                .compileAllSql(List.of(
                        "CREATE SQL FUNCTION after_return",
                        "RETURN",
                        "SELECT 1"
                )));

        assertTrue(error.getMessage().contains("sql after return is invalid"));
    }

    @Test
    void returningUnknownTableIsRejected() {
        Exception error = assertCompileError(
                "unknown_return_table",
                "RETURN missing_table"
        );

        assertTrue(error.getMessage().contains("return table not found"));
    }

    @Test
    void returningANonCacheTableIsRejected() {
        CalciteSchema compileSchema = CalciteSchema.createRootSchema(false);
        compileSchema.add("regular_table", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder().add("id", SqlTypeName.INTEGER).build();
            }
        });
        FunctionCompiler compiler = new FunctionCompiler(compileSchema, new CompileManager());

        Exception error = assertThrows(Exception.class, () -> compiler.compileAllSql(List.of(
                "CREATE SQL FUNCTION non_cache_return",
                "RETURN regular_table"
        )));

        assertTrue(error.getMessage().contains("return table is not cache table"));
    }

    @Test
    void asyncReturnCallIsRejected() {
        Exception error = assertCompileError(
                "async_return_call",
                "RETURN CALL anything() ASYNC"
        );

        assertTrue(error.getMessage().contains("async function is not supported in return statement"));
    }

    @Test
    void ifRejectsElseOnlyReturn() {
        Exception error = assertCompileError(
                "else_only_return",
                "IF (SELECT FALSE) THEN (SELECT 1 AS id) ELSE (RETURN SELECT 2 AS id)",
                "RETURN SELECT 2 AS id"
        );

        assertTrue(error.getMessage().contains("IF with RETURN"));
    }

    @Test
    void ifRejectsReturningThenAndNonReturningElse() {
        Exception error = assertCompileError(
                "mixed_return_else",
                "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id) ELSE (SELECT 2 AS id)",
                "RETURN SELECT 1 AS id"
        );

        assertTrue(error.getMessage().contains("IF with RETURN"));
    }

    @Test
    void ifRejectsIncompatibleReturnBranchSchemas() {
        Exception error = assertCompileError(
                "branch_schema_mismatch",
                "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id) "
                        + "ELSE (RETURN SELECT CAST(2 AS BIGINT) AS id)",
                "RETURN SELECT 1 AS id"
        );

        assertTrue(error.getMessage().contains("field type not equal"));
    }

    @Test
    void functionRejectsEmptyAndDataReturnsAcrossNodes() {
        Exception error = assertCompileError(
                "empty_data_mismatch",
                "IF (SELECT TRUE) THEN (RETURN)",
                "RETURN SELECT 1 AS id"
        );

        assertTrue(error.getMessage().contains("all return data or all return empty"));
    }

    @Test
    void functionRejectsDifferentReturnSchemasAcrossNodes() {
        Exception error = assertCompileError(
                "node_schema_mismatch",
                "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id)",
                "RETURN SELECT CAST(1 AS BIGINT) AS id"
        );

        assertTrue(error.getMessage().contains("field type not equal"));
    }

    @Test
    void matchingReturnSchemasAcrossNodesCompileSuccessfully() throws Exception {
        SqlFunctionBindable function = compile(
                "matching_return_schemas",
                "IF (SELECT FALSE) THEN (RETURN SELECT 1 AS id)",
                "RETURN SELECT 2 AS id"
        );

        assertFalse(function.getReturnDataFields().isEmpty());
        assertEquals(2, function.bind(schema, new ExecuteContextImpl()).toList().get(0)[0]);
    }

    private SqlFunctionBindable compile(String name, String... body) throws Exception {
        List<String> sql = new ArrayList<>();
        sql.add("CREATE SQL FUNCTION " + name);
        sql.addAll(Arrays.asList(body));
        return new CompileManager().compileSqlFunction(name, sql);
    }

    private Exception assertCompileError(String name, String... body) {
        return assertThrows(Exception.class, () -> compile(name, body));
    }
}
