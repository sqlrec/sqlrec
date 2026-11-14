package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlTestCase {
    private static final Logger log = LoggerFactory.getLogger(SqlTestCase.class);

    public String sql;
    public List<Object[]> expectedResult;
    /** Expected outer exception type; a non-null message must occur in its cause chain. */
    public Exception expectedException;
    private Throwable expectedRootCause;
    public String expectedLogicalPlan;
    public String expectedPhysicalPlan;
    public String expectedJavaExpression;
    public boolean debugOutput = true;

    public SqlTestCase(String sql) {
        this.sql = sql;
    }

    public SqlTestCase(String sql, List<Object[]> expectedResult) {
        this.sql = sql;
        this.expectedResult = expectedResult;
    }

    public SqlTestCase(String sql, List<Object[]> expectedResult, Exception expectedException) {
        this.sql = sql;
        this.expectedResult = expectedResult;
        this.expectedException = expectedException;
    }

    public SqlTestCase(String sql, List<Object[]> expectedResult,
                       String expectedLogicalPlan, String expectedPhysicalPlan, String expectedJavaExpression) {
        this.sql = sql;
        this.expectedResult = expectedResult;
        this.expectedLogicalPlan = expectedLogicalPlan;
        this.expectedPhysicalPlan = expectedPhysicalPlan;
        this.expectedJavaExpression = expectedJavaExpression;
    }

    public SqlTestCase setDebugOutput(boolean debugOutput) {
        this.debugOutput = debugOutput;
        return this;
    }

    /** Optionally require a specific root cause, including its message substring. */
    public SqlTestCase setExpectedRootCause(Throwable expectedRootCause) {
        this.expectedRootCause = java.util.Objects.requireNonNull(expectedRootCause);
        return this;
    }

    public void checkBindable(BindableInterface bindable) {
        checkPlan("Logical plan", expectedLogicalPlan, bindable.getLogicalPlan());
        checkPlan("Physical plan", expectedPhysicalPlan, bindable.getPhysicalPlan());
        checkPlan("Java expression", expectedJavaExpression, bindable.getJavaExpression());
    }

    private void checkPlan(String name, String expected, String actual) {
        if (expected != null && !normalizeString(expected).equals(normalizeString(actual))) {
            throw new AssertionError(name + " mismatch for SQL: " + sql
                    + "\nExpected:\n" + expected + "\nActual:\n" + actual);
        }
    }

    private String normalizeString(String str) {
        if (str == null) {
            return null;
        }
        return str.replaceAll("\\s+", "");
    }

    public void test(CalciteSchema schema) throws Exception {
        test(schema, new ExecuteContextImpl(schema));
    }

    public void test(CalciteSchema schema, ExecuteContext executeContext) throws Exception {
        log.info(sql);
        Exception actualException = null;
        List<Object[]> actualResults = new ArrayList<>();
        try {
            SqlNode flinkSqlNode = CompileManager.parseSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(
                    flinkSqlNode, schema, Consts.DEFAULT_SCHEMA_NAME, sql
            );

            if (bindable.getLogicalPlan() != null) {
                if (debugOutput) {
                    log.info("=== Logical Plan ===");
                    log.info(bindable.getLogicalPlan());
                    log.info("=== Physical Plan ===");
                    log.info(bindable.getPhysicalPlan());
                    log.info("=== Java Expression ===");
                    log.info(bindable.getJavaExpression());
                }
            }
            checkBindable(bindable);
            Enumerable<Object[]> enumerable = bindable.bind(schema, executeContext);
            // Failures may be deferred until the enumerable is consumed.
            if (enumerable != null) {
                actualResults = enumerable.toList();
            }
        } catch (Exception e) {
            actualException = e;
        }

        if (expectedException != null || expectedRootCause != null) {
            if (actualException == null) {
                throw new AssertionError("Expected SQL execution to fail: " + sql);
            }
            if (expectedException != null) {
                if (!expectedException.getClass().isInstance(actualException)) {
                    throw new AssertionError("Expected " + expectedException.getClass().getName()
                            + " but got " + actualException.getClass().getName() + " for SQL: " + sql,
                            actualException);
                }
                // Execution proxies may add a wrapper; retain the original diagnostic.
                requireMessage(expectedException.getMessage(), actualException, true);
            }
            if (expectedRootCause != null) {
                Throwable rootCause = actualException;
                while (rootCause.getCause() != null) {
                    rootCause = rootCause.getCause();
                }
                if (!expectedRootCause.getClass().isInstance(rootCause)) {
                    throw new AssertionError("Expected root cause " + expectedRootCause.getClass().getName()
                            + " but got " + rootCause.getClass().getName() + " for SQL: " + sql,
                            actualException);
                }
                requireMessage(expectedRootCause.getMessage(), rootCause, false);
            }
            return;
        }
        if (actualException != null) {
            throw new AssertionError("SQL execution failed: " + sql, actualException);
        }

        for (Object[] result : actualResults) {
            log.info(java.util.Arrays.toString(result));
        }

        DataCheckUtils.checkResultEqual(actualResults, expectedResult);
    }

    private void requireMessage(String expected, Throwable actual, boolean searchCauses) {
        if (expected == null) {
            return;
        }
        for (Throwable candidate = actual; candidate != null;
             candidate = searchCauses ? candidate.getCause() : null) {
            if (candidate.getMessage() != null && candidate.getMessage().contains(expected)) {
                return;
            }
        }
        throw new AssertionError("Expected exception message to contain '" + expected
                + "' for SQL: " + sql, actual);
    }
}
