package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.schema.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UnionDependencyTest {
    private boolean originalParallelism;

    @BeforeEach
    void setUp() {
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(ConcurrentCalciteSchema.createRootSchema());
        JavaFunctionUtils.setSkipHmsQuery(true);
        JavaFunctionUtils.registerTableFunction("default", "dependency_fail", FailingFunction.class);
    }

    @AfterEach
    void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @Test
    void wrappedFailuresRecoverInUnionAndTimeinAcrossFunctionCalls() throws Exception {
        JavaFunctionUtils.registerTableFunction("default", "throwable_fail", ThrowableFunction.class);
        for (String kind : List.of("error", "interruption")) {
            new CompileManager().compileSqlFunction("throwable_child", List.of(
                    "CREATE SQL FUNCTION throwable_child",
                    "DEFINE INPUT TABLE input1(id integer)",
                    "RETURN CALL throwable_fail(input1, '" + kind + "') LIKE input1"));
            for (String call : List.of("throwable_fail(seed_table, '" + kind + "') LIKE seed_table",
                    "throwable_child(seed_table)",
                    "GET_OR_DEFAULT('callee', 'throwable_child')(seed_table) LIKE seed_table")) {
                SqlFunctionBindable union = compile(
                        "CACHE TABLE seed_table AS SELECT 1 AS id",
                        "CACHE TABLE branch_table AS CALL " + call,
                        "RETURN SELECT * FROM branch_table UNION ALL SELECT 2 AS id");
                assertTrue(union.getBindableList().get(1).isIgnoreException());
                assertResultInBothModes(union, List.of(2));

                SqlFunctionBindable timein = compile(
                        "CACHE TABLE seed_table AS SELECT 1 AS id",
                        "IF TIMEIN (SELECT 5000) THEN (CACHE TABLE branch_table AS CALL " + call + ") "
                                + "ELSE (CACHE TABLE branch_table AS SELECT 2 AS id)",
                        "RETURN branch_table");
                assertResultInBothModes(timein, List.of(2));
            }
        }
    }

    @Test
    void directNonUnionConsumerCannotBeRemovedByTransitiveReduction() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "CACHE TABLE output_table AS SELECT s.id FROM source_table s "
                        + "JOIN merged m ON s.id = m.id",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void readBeforeUnionOverwriteIsNotAUnionInput() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE base AS SELECT 1 AS id",
                "CACHE TABLE unused_table AS CALL dependency_fail(base)",
                "CACHE TABLE base AS SELECT 2 AS id UNION ALL SELECT 3 AS id",
                "RETURN base");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void writeBeforeUnionOverwriteIsNotAUnionInput() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE source_table AS SELECT 2 AS id UNION ALL SELECT 3 AS id",
                "RETURN source_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void multipleWritesKeepAllVersionsNonRecoverable() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "CACHE TABLE source_table AS SELECT 2 AS id",
                "RETURN SELECT s.id FROM source_table s JOIN merged m ON s.id = m.id");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(3).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void selfOverwriteDoesNotRecoverAFailedEarlierWrite() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE source_table AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "RETURN source_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(2).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void nonParallelAssertionRemainsANonUnionConsumer() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "ASSERT SELECT COUNT(*) > 0 FROM source_table",
                "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "RETURN merged");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void ifConditionIsRequiredEvenWhenBothBranchesAreUnions() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE guard_table AS CALL dependency_fail(seed_table)",
                "IF (SELECT COUNT(*) > 0 FROM guard_table) "
                        + "THEN (CACHE TABLE output_table AS SELECT 1 AS id UNION ALL SELECT 2 AS id) "
                        + "ELSE (CACHE TABLE output_table AS SELECT 3 AS id UNION ALL SELECT 4 AS id)",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void ifConditionIsRequiredWhenTheIfResultFeedsALaterUnion() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE guard_table AS CALL dependency_fail(seed_table)",
                "IF (SELECT COUNT(*) > 0 FROM guard_table) "
                        + "THEN (CACHE TABLE branch_table AS SELECT 1 AS id) "
                        + "ELSE (CACHE TABLE branch_table AS SELECT 2 AS id)",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 3 AS id");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void aConditionReadRemainsRequiredWhenItAlsoFeedsAUnionBranch() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE Guard_Table AS CALL dependency_fail(seed_table)",
                "IF (SELECT COUNT(*) > 0 FROM GUARD_TABLE) "
                        + "THEN (CACHE TABLE output_table AS SELECT * FROM guard_table UNION ALL SELECT 2 AS id) "
                        + "ELSE (CACHE TABLE output_table AS SELECT * FROM guard_table UNION ALL SELECT 3 AS id)",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void timeinDurationInputIsRequiredEvenWhenBothBranchesAreUnions() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE guard_table AS CALL dependency_fail(seed_table)",
                "IF TIMEIN (SELECT COALESCE(MAX(id), 0) FROM guard_table) "
                        + "THEN (CACHE TABLE output_table AS SELECT 1 AS id UNION ALL SELECT 2 AS id) "
                        + "ELSE (CACHE TABLE output_table AS SELECT 3 AS id UNION ALL SELECT 4 AS id)",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void conditionProducersStayRequiredThroughIntermediateTables() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE guard_table AS SELECT COUNT(*) > 0 AS flag FROM source_table",
                "IF (SELECT flag FROM guard_table) "
                        + "THEN (CACHE TABLE output_table AS SELECT 1 AS id UNION ALL SELECT 2 AS id) "
                        + "ELSE (CACHE TABLE output_table AS SELECT 3 AS id UNION ALL SELECT 4 AS id)",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(2).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void unionInputsUsedToComputeAConditionCannotDegrade() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE guard_table AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "IF (SELECT COUNT(*) > 1 FROM guard_table) "
                        + "THEN (CACHE TABLE output_table AS SELECT 1 AS id UNION ALL SELECT 2 AS id) "
                        + "ELSE (CACHE TABLE output_table AS SELECT 3 AS id UNION ALL SELECT 4 AS id)",
                "RETURN output_table");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(2).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void repeatedWritesRemainProtectedEvenWhenTheConditionReadsTheLatestVersion() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "CACHE TABLE source_table AS SELECT 2 AS id",
                "IF (SELECT COUNT(*) > 0 FROM source_table) "
                        + "THEN (RETURN SELECT s.id FROM source_table s JOIN merged m ON s.id = m.id) "
                        + "ELSE (RETURN SELECT 3 AS id)",
                "RETURN");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(3).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void returnedUnionsDoNotMakeTheIfConditionOptional() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE guard_table AS CALL dependency_fail(seed_table)",
                "IF (SELECT COUNT(*) > 0 FROM guard_table) "
                        + "THEN (RETURN SELECT 1 AS id UNION ALL SELECT 2 AS id) "
                        + "ELSE (RETURN SELECT 3 AS id UNION ALL SELECT 4 AS id)",
                "RETURN");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void inputsReadByIfBranchesRemainProtectedEvenWithAHealthyCondition() throws Exception {
        for (boolean condition : List.of(false, true)) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE optional_table AS CALL dependency_fail(seed_table)",
                    "IF (SELECT " + condition + ") "
                            + "THEN (CACHE TABLE output_table AS SELECT * FROM optional_table UNION ALL SELECT 2 AS id) "
                            + "ELSE (CACHE TABLE output_table AS SELECT * FROM optional_table UNION ALL SELECT 3 AS id)",
                    "RETURN output_table");

            assertFalse(function.getBindableList().get(1).isIgnoreException());
            assertFalse(function.getBindableList().get(2).isIgnoreException());
            assertFailsInBothModes(function);
        }
    }

    @Test
    void conditionalOverwriteKeepsTheOldProducerRequired() throws Exception {
        for (boolean condition : List.of(false, true)) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                    "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                    "IF (SELECT " + condition + ") THEN (CACHE TABLE source_table AS SELECT 2 AS id)",
                    "RETURN SELECT s.id FROM source_table s JOIN merged m ON s.id = m.id");

            // Multiple writes are protected regardless of which branch executes.
            assertFalse(function.getBindableList().get(1).isIgnoreException());
            assertFailsInBothModes(function);
        }
    }

    @Test
    void ifCacheBranchesCanDifferOnlyInTableNameCase() throws Exception {
        for (boolean condition : List.of(false, true)) {
            for (String result : List.of("RETURN result_table", "RETURN SELECT * FROM result_table")) {
                SqlFunctionBindable function = compile(
                        "IF (SELECT " + condition + ") THEN (CACHE TABLE Result_Table AS SELECT 1 AS id) "
                                + "ELSE (CACHE TABLE RESULT_TABLE AS SELECT 2 AS id)",
                        result);

                assertResultInBothModes(function, List.of(condition ? 1 : 2));
            }
        }
    }

    @Test
    void differentlyCasedCacheWritesReplaceTheSameTable() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE Result_Table AS SELECT 1 AS id",
                "CACHE TABLE RESULT_TABLE AS SELECT 2 AS id",
                "RETURN SELECT * FROM Result_Table");

        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void caseInsensitiveIfBranchesReplaceAnExistingCacheTable() throws Exception {
        for (boolean condition : List.of(false, true)) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE ReSuLt_TaBlE AS SELECT 0 AS id",
                    "IF (SELECT " + condition + ") THEN (CACHE TABLE Result_Table AS SELECT 1 AS id) "
                            + "ELSE (CACHE TABLE RESULT_TABLE AS SELECT 2 AS id)",
                    "RETURN SELECT * FROM result_table");

            assertResultInBothModes(function, List.of(condition ? 1 : 2));
        }
    }

    @Test
    void timeinCacheBranchesCanDifferOnlyInTableNameCase() throws Exception {
        for (int timeout : List.of(0, 5000)) {
            SqlFunctionBindable function = compile(
                    "IF TIMEIN (SELECT " + timeout + ") THEN (CACHE TABLE Result_Table AS SELECT 1 AS id) "
                            + "ELSE (CACHE TABLE RESULT_TABLE AS SELECT 2 AS id)",
                    "RETURN SELECT * FROM result_table");

            assertResultInBothModes(function, List.of(timeout == 0 ? 2 : 1));
        }

        SqlFunctionBindable fallback = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "IF TIMEIN (SELECT 5000) THEN (CACHE TABLE Result_Table AS CALL dependency_fail(seed_table)) "
                        + "ELSE (CACHE TABLE RESULT_TABLE AS SELECT 2 AS id)",
                "RETURN SELECT * FROM result_table");
        assertResultInBothModes(fallback, List.of(2));
    }

    @Test
    void conditionalOverwritePreservesOrReplacesHealthyDataAtRuntime() throws Exception {
        for (boolean condition : List.of(false, true)) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE source_table AS SELECT 1 AS id",
                    "IF (SELECT " + condition + ") THEN (CACHE TABLE source_table AS SELECT 2 AS id)",
                    "RETURN source_table");

            assertResultInBothModes(function, List.of(condition ? 2 : 1));
        }
    }

    @Test
    void aFalseIfWithoutAnExistingTableStillProvidesAnEmptyTable() throws Exception {
        SqlFunctionBindable function = compile(
                "IF (SELECT FALSE) THEN (CACHE TABLE source_table AS SELECT 2 AS id)",
                "RETURN source_table");

        assertResultInBothModes(function, List.of());
    }

    @Test
    void bothCacheBranchesStillReplaceHealthyData() throws Exception {
        for (boolean condition : List.of(false, true)) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE source_table AS SELECT 0 AS id",
                    "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id UNION ALL SELECT 3 AS id",
                    "IF (SELECT " + condition + ") THEN (CACHE TABLE source_table AS SELECT 2 AS id) "
                            + "ELSE (CACHE TABLE source_table AS SELECT 3 AS id)",
                    "RETURN SELECT s.id FROM source_table s JOIN merged m ON s.id = m.id");

            assertFalse(function.getBindableList().get(1).isIgnoreException());
            assertResultInBothModes(function, List.of(condition ? 2 : 3));
        }
    }

    @Test
    void laterUnconditionalWritesDoNotMakeRepeatedWritersRecoverable() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE source_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE merged AS SELECT * FROM source_table UNION ALL SELECT 2 AS id",
                "IF (SELECT FALSE) THEN (CACHE TABLE source_table AS SELECT 3 AS id)",
                "CACHE TABLE source_table AS SELECT 2 AS id",
                "RETURN SELECT s.id FROM source_table s JOIN merged m ON s.id = m.id");

        assertFalse(function.getBindableList().get(1).isIgnoreException());
        assertFailsInBothModes(function);
    }

    @Test
    void aCalleeConditionDoesNotProtectAnOptionalCallerInput() throws Exception {
        new CompileManager().compileSqlFunction("condition_child", List.of(
                "CREATE SQL FUNCTION condition_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "CACHE TABLE guard_table AS SELECT COUNT(*) > 0 AS flag FROM input1",
                "IF (SELECT flag FROM guard_table) THEN (RETURN SELECT 1 AS id) ELSE (RETURN SELECT 2 AS id)",
                "RETURN"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE actual_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE branch_table AS CALL condition_child(actual_table)",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 3 AS id");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertResultInBothModes(function, List.of(2, 3));
    }

    @Test
    void aDynamicCalleeConditionDoesNotProtectAnOptionalCallerInput() throws Exception {
        new CompileManager().compileSqlFunction("dynamic_condition_child", List.of(
                "CREATE SQL FUNCTION dynamic_condition_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "IF (SELECT COUNT(*) > 0 FROM input1) THEN (RETURN SELECT 1 AS id) ELSE (RETURN SELECT 2 AS id)",
                "RETURN"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE actual_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE branch_table AS CALL GET_OR_DEFAULT('callee', 'dynamic_condition_child')"
                        + "(actual_table) LIKE actual_table",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 3 AS id");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertResultInBothModes(function, List.of(2, 3));
    }

    @Test
    void aFailureInsideAConditionalCalleeCanBeRecoveredByItsCaller() throws Exception {
        SqlFunctionBindable child = compileFailingConditionalChild();
        assertFalse(child.getBindableList().getFirst().isIgnoreException());
        for (String call : List.of("failing_if_child(seed_table)",
                "GET_OR_DEFAULT('callee', 'failing_if_child')(seed_table) LIKE seed_table")) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE branch_table AS CALL " + call,
                    "RETURN SELECT * FROM branch_table UNION ALL SELECT 3 AS id");

            assertTrue(function.getBindableList().get(1).isIgnoreException());
            assertResultInBothModes(function, List.of(3));
        }
    }

    @Test
    void failedCallsRemainRequiredWhenReturnedDirectlyOrUsedOutsideUnion() throws Exception {
        compileFailingConditionalChild();
        for (String call : List.of("failing_if_child(seed_table)",
                "GET_OR_DEFAULT('callee', 'failing_if_child')(seed_table) LIKE seed_table")) {
            SqlFunctionBindable returned = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE branch_table AS CALL " + call,
                    "RETURN branch_table");
            assertFalse(returned.getBindableList().get(1).isIgnoreException());
            assertFailsInBothModes(returned);

            SqlFunctionBindable shared = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE branch_table AS CALL " + call,
                    "CACHE TABLE merged AS SELECT * FROM branch_table UNION ALL SELECT 3 AS id",
                    "RETURN SELECT b.id FROM branch_table b JOIN merged m ON b.id = m.id");
            assertFalse(shared.getBindableList().get(1).isIgnoreException());
            assertFailsInBothModes(shared);
        }
    }

    @Test
    void aParentConditionUsesTheCalleesRecoveredResult() throws Exception {
        compileRecoverableChild();
        for (String call : List.of("recoverable_child(seed_table)",
                "GET_OR_DEFAULT('callee', 'recoverable_child')(seed_table) LIKE seed_table")) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE guard_table AS CALL " + call,
                    "IF (SELECT COUNT(*) > 1 FROM guard_table) THEN (RETURN SELECT 1 AS id) "
                            + "ELSE (RETURN SELECT 2 AS id)",
                    "RETURN");

            assertFalse(function.getBindableList().get(1).isIgnoreException());
            assertResultInBothModes(function, List.of(2));
        }
    }

    @Test
    void anUnrecoveredCalleeFailureStillFailsAParentCondition() throws Exception {
        compileFailingConditionalChild();
        for (String call : List.of("failing_if_child(seed_table)",
                "GET_OR_DEFAULT('callee', 'failing_if_child')(seed_table) LIKE seed_table")) {
            SqlFunctionBindable function = compile(
                    "CACHE TABLE seed_table AS SELECT 1 AS id",
                    "CACHE TABLE guard_table AS CALL " + call,
                    "IF (SELECT COUNT(*) > 0 FROM guard_table) THEN (CACHE TABLE result_table AS SELECT 2 AS id) "
                            + "ELSE (CACHE TABLE result_table AS SELECT 3 AS id)",
                    "RETURN SELECT * FROM result_table UNION ALL SELECT 4 AS id");

            assertFalse(function.getBindableList().get(1).isIgnoreException());
            assertFailsInBothModes(function);
        }
    }

    @Test
    void optionalDynamicCallsAllowRecoveryInsideTheirCallees() throws Exception {
        compileRecoverableChild();
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL GET_OR_DEFAULT('callee', 'recoverable_child')"
                        + "(seed_table) LIKE seed_table",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 3 AS id");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertResultInBothModes(function, List.of(2, 3));
    }

    @Test
    void timeinStillFallsBackToElseAfterAnOrdinaryBranchFailure() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "IF TIMEIN (SELECT 10000) THEN (CACHE TABLE branch_table AS CALL dependency_fail(seed_table)) "
                        + "ELSE (CACHE TABLE branch_table AS SELECT 2 AS id)",
                "RETURN branch_table");

        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void setStatementsDoNotDisableRecoveryInUnconditionalFunctions() throws Exception {
        SqlFunctionBindable function = compile(
                "SET IGNORE_UNION_EXCEPTION=true",
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 2 AS id");

        assertTrue(function.getBindableList().get(2).isIgnoreException());
        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void anUnrelatedIfDoesNotDisableUnionRecovery() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE merged AS SELECT * FROM branch_table UNION ALL SELECT 2 AS id",
                "IF (SELECT TRUE) THEN (CACHE TABLE side_table AS SELECT 9 AS id)",
                "RETURN merged");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(3).isIgnoreException());
        assertFalse(function.getBindableList().get(3).isUnionSql());
        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void unrelatedRepeatedWritesDoNotDisableUnionRecovery() throws Exception {
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE Side_Table AS SELECT 8 AS id",
                "CACHE TABLE SIDE_TABLE AS SELECT 9 AS id",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 2 AS id");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertFalse(function.getBindableList().get(2).isIgnoreException());
        assertFalse(function.getBindableList().get(3).isIgnoreException());
        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void anUnrelatedConditionalCalleeDoesNotDisableUnionRecovery() throws Exception {
        new CompileManager().compileSqlFunction("healthy_if_child", List.of(
                "CREATE SQL FUNCTION healthy_if_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "IF (SELECT COUNT(*) > 0 FROM input1) THEN (RETURN input1) ELSE (RETURN SELECT 2 AS id)",
                "RETURN"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE call_table AS CALL healthy_if_child(seed_table)",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT * FROM call_table");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertTrue(function.getBindableList().get(2).isIgnoreException());
        assertResultInBothModes(function, List.of(1));
    }

    @Test
    void anUnrelatedDynamicCallDoesNotDisableUnionRecovery() throws Exception {
        new CompileManager().compileSqlFunction("healthy_dynamic_child", List.of(
                "CREATE SQL FUNCTION healthy_dynamic_child",
                "DEFINE INPUT TABLE input1(id integer)", "RETURN input1"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE call_table AS CALL GET_OR_DEFAULT('callee', 'healthy_dynamic_child')"
                        + "(seed_table) LIKE seed_table",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT * FROM call_table");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertTrue(function.getBindableList().get(2).isIgnoreException());
        assertResultInBothModes(function, List.of(1));
    }

    @Test
    void anUnrelatedIfDoesNotDisableRecoveryInsideANormalCallee() throws Exception {
        compileRecoverableChild();
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL recoverable_child(seed_table)",
                "IF (SELECT TRUE) THEN (CACHE TABLE side_table AS SELECT 9 AS id)",
                "RETURN branch_table");

        assertResultInBothModes(function, List.of(2));
    }

    @Test
    void aFailureInsideARepeatedWriteCalleeCanBeRecoveredByItsCaller() throws Exception {
        SqlFunctionBindable child = new CompileManager().compileSqlFunction("repeated_write_child", List.of(
                "CREATE SQL FUNCTION repeated_write_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "CACHE TABLE branch_table AS CALL dependency_fail(input1)",
                "CACHE TABLE branch_table AS SELECT 2 AS id", "RETURN branch_table"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE call_table AS CALL repeated_write_child(seed_table)",
                "RETURN SELECT * FROM call_table UNION ALL SELECT 3 AS id");

        assertFalse(child.getBindableList().getFirst().isIgnoreException());
        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertResultInBothModes(function, List.of(3));
    }

    @Test
    void anUnrelatedRepeatedWriteCalleeDoesNotDisableUnionRecovery() throws Exception {
        new CompileManager().compileSqlFunction("healthy_repeated_write_child", List.of(
                "CREATE SQL FUNCTION healthy_repeated_write_child",
                "CACHE TABLE branch_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS SELECT 2 AS id", "RETURN branch_table"));
        SqlFunctionBindable function = compile(
                "CACHE TABLE seed_table AS SELECT 1 AS id",
                "CACHE TABLE branch_table AS CALL dependency_fail(seed_table)",
                "CACHE TABLE call_table AS CALL healthy_repeated_write_child()",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT * FROM call_table");

        assertTrue(function.getBindableList().get(1).isIgnoreException());
        assertTrue(function.getBindableList().get(2).isIgnoreException());
        assertResultInBothModes(function, List.of(2));
    }

    private void compileRecoverableChild() throws Exception {
        new CompileManager().compileSqlFunction("recoverable_child", List.of(
                "CREATE SQL FUNCTION recoverable_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "CACHE TABLE branch_table AS CALL dependency_fail(input1)",
                "RETURN SELECT * FROM branch_table UNION ALL SELECT 2 AS id"));
    }

    private SqlFunctionBindable compileFailingConditionalChild() throws Exception {
        return new CompileManager().compileSqlFunction("failing_if_child", List.of(
                "CREATE SQL FUNCTION failing_if_child",
                "DEFINE INPUT TABLE input1(id integer)",
                "IF (SELECT TRUE) THEN (RETURN CALL dependency_fail(input1)) ELSE (RETURN input1)",
                "RETURN"));
    }

    private SqlFunctionBindable compile(String... statements) throws Exception {
        List<String> sql = new ArrayList<>();
        sql.add("CREATE SQL FUNCTION union_dependency_test");
        sql.addAll(List.of(statements));
        return new CompileManager().compileSqlFunction("union_dependency_test", sql);
    }

    private void assertFailsInBothModes(SqlFunctionBindable function) {
        for (boolean parallel : List.of(false, true)) {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> function.bind(schema, new ExecuteContextImpl(schema)),
                    "parallel=" + parallel);
            Throwable cause = error;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertEquals("dependency failure", cause.getMessage());
        }
    }

    private void assertResultInBothModes(SqlFunctionBindable function, List<Integer> expected) {
        for (boolean parallel : List.of(false, true)) {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
            CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
            assertEquals(expected, function.bind(schema, new ExecuteContextImpl(schema))
                    .select(row -> (Integer) row[0]).toList(), "parallel=" + parallel);
        }
    }

    public static class ThrowableFunction {
        public CacheTable evaluate(CacheTable input, String kind) throws InterruptedException {
            if ("error".equals(kind)) {
                throw new AssertionError("dependency error");
            }
            throw new InterruptedException("dependency interruption");
        }
    }

    public static class FailingFunction {
        public CacheTable evaluate(CacheTable input) {
            // Compilation infers the output schema using an empty input table.
            if (input.scan(null).count() == 0) {
                return input;
            }
            throw new IllegalStateException("dependency failure");
        }
    }
}
