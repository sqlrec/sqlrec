package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CalciteBindable;
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
    public Exception expectedException;
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

    public void checkBindable(CalciteBindable bindable) {
        if (expectedLogicalPlan != null) {
            String actualLogicalPlan = bindable.getLogicalPlan();
            if (!normalizeString(expectedLogicalPlan).equals(normalizeString(actualLogicalPlan))) {
                log.error("Logical plan mismatch:");
                log.error("  Expected (length={}):\n{}", expectedLogicalPlan.length(), expectedLogicalPlan);
                log.error("  Actual (length={}):\n{}", actualLogicalPlan.length(), actualLogicalPlan);
                assert false : "Logical plan mismatch";
            }
        }
        if (expectedPhysicalPlan != null) {
            String actualPhysicalPlan = bindable.getPhysicalPlan();
            if (!normalizeString(expectedPhysicalPlan).equals(normalizeString(actualPhysicalPlan))) {
                log.error("Physical plan mismatch:");
                log.error("  Expected (length={}):\n{}", expectedPhysicalPlan.length(), expectedPhysicalPlan);
                log.error("  Actual (length={}):\n{}", actualPhysicalPlan.length(), actualPhysicalPlan);
                assert false : "Physical plan mismatch";
            }
        }
        if (expectedJavaExpression != null) {
            String actualJavaExpression = bindable.getJavaExpression();
            if (!normalizeString(expectedJavaExpression).equals(normalizeString(actualJavaExpression))) {
                log.error("Java expression mismatch:");
                log.error("  Expected (length={}):\n{}", expectedJavaExpression.length(), expectedJavaExpression);
                log.error("  Actual (length={}):\n{}", actualJavaExpression.length(), actualJavaExpression);
                assert false : "Java expression mismatch";
            }
        }
    }

    private String normalizeString(String str) {
        if (str == null) {
            return null;
        }
        return str.replaceAll("\\s+", "");
    }

    public void test(CalciteSchema schema) throws Exception {
        test(schema, new ExecuteContextImpl());
    }

    public void test(CalciteSchema schema, ExecuteContext executeContext) throws Exception {
        log.info(sql);
        Exception actualException = null;
        Enumerable enumerable = null;
        try {
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = new CompileManager().compileSql(
                    flinkSqlNode, schema, Consts.DEFAULT_SCHEMA_NAME, sql
            );

            if (bindable instanceof CalciteBindable) {
                CalciteBindable calciteBindable = (CalciteBindable) bindable;
                if (debugOutput) {
                    log.info("=== Logical Plan ===");
                    log.info(calciteBindable.getLogicalPlan());
                    log.info("=== Physical Plan ===");
                    log.info(calciteBindable.getPhysicalPlan());
                    log.info("=== Java Expression ===");
                    log.info(calciteBindable.getJavaExpression());
                }
                checkBindable(calciteBindable);
            }
            enumerable = bindable.bind(schema, executeContext);
        } catch (Exception e) {
            actualException = e;
        }

        if (expectedException != null) {
            assert actualException != null;
            assert expectedException.getClass().isAssignableFrom(actualException.getClass());
        } else {
            if (actualException != null) {
                log.error("Exception during execution:", actualException);
            }
            assert actualException == null;
        }

        List<Object[]> actualResults = new ArrayList<>();
        if (enumerable != null) {
            actualResults = enumerable.toList();
            for (Object[] result : actualResults) {
                log.info(java.util.Arrays.toString(result));
            }
        } else {
            log.info("no result");
        }

        DataCheckUtils.checkResultEqual(actualResults, expectedResult);
    }
}
