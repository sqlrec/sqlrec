package com.sqlrec.utils;

import com.sqlrec.common.config.Consts;
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
    public Exception expectedException;

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

    public void checkResult(List<Object[]> actualResult) {
        if (expectedResult == null) {
            return;
        }

        assert actualResult != null : "actualResult is null";
        assert actualResult.size() == expectedResult.size() : 
            "size mismatch: expected " + expectedResult.size() + ", actual " + actualResult.size();
        for (int i = 0; i < actualResult.size(); i++) {
            Object[] actualRow = actualResult.get(i);
            Object[] expectedRow = expectedResult.get(i);
            if (!java.util.Arrays.deepEquals(actualRow, expectedRow)) {
                log.error("Row {} mismatch:", i);
                log.error("  Expected: {}", java.util.Arrays.deepToString(expectedRow));
                log.error("  Actual:   {}", java.util.Arrays.deepToString(actualRow));
                assert false : "Row " + i + " mismatch";
            }
        }
    }

    public void test(CalciteSchema schema) throws Exception {
        test(schema, new ExecuteContextImpl());
    }

    public void test(CalciteSchema schema, ExecuteContext executeContext) throws Exception {
        log.info(sql);
        SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
        BindableInterface bindable = new CompileManager().compileSql(flinkSqlNode, schema,
                Consts.DEFAULT_SCHEMA_NAME);

        Exception actualException = null;
        Enumerable enumerable = null;
        try {
            enumerable = bindable.bind(schema, executeContext);
        } catch (Exception e) {
            actualException = e;
        }

        if (expectedException != null) {
            assert actualException != null;
            assert expectedException.getClass().isAssignableFrom(actualException.getClass());
        } else {
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

        checkResult(actualResults);
    }
}
