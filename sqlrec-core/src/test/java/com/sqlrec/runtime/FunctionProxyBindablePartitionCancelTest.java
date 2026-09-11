package com.sqlrec.runtime;

import com.sqlrec.utils.TypeSupportTest;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class FunctionProxyBindablePartitionCancelTest {

    public static final AtomicBoolean CANCEL_OBSERVED = new AtomicBoolean(false);

    /**
     * Each partition polls the context it receives and exits upon cancellation.
     * Verifies that after an ancestor cancels the execution tree, running partition tasks observe it via the context and terminate.
     */
    public static class PartitionPollFun {
        public CacheTable evaluate(CacheTable input, ExecuteContext ctx) {
            while (!ctx.isCancelled()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // ignore interrupts, keep polling the cancellation flag
                }
            }
            CANCEL_OBSERVED.set(true);
            throw new RuntimeException("partition observed cancellation");
        }
    }

    @Test
    public void testPartitionTasksObserveAncestorCancellation() throws Exception {
        CANCEL_OBSERVED.set(false);
        ExecuteContext executeContext = new ExecuteContextImpl();
        executeContext.setVariable("IGNORE_PARTITION_EXCEPTION", "true");
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TypeSupportTest.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "partition_poll_fun", PartitionPollFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                // prepare source data / like table before scheduling cancellation, so
                // compilation speed cannot make this test flaky.
                new SqlTestCase("cache table t1 as select * from myTable"),
                new SqlTestCase("cache table r0 as select int_type from myTable")
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }

        // cancel the whole execution tree externally after 200ms (simulating an ancestor timeout/failure cancel)
        Thread canceller = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            executeContext.cancel();
        });
        canceller.start();

        try {
            // Tolerant mode must not swallow ancestor cancellation. All partition tasks
            // should still observe it and terminate.
            new SqlTestCase(
                    "cache table r1 as call partition_poll_fun(t1) like r0 partition by t1 size 1",
                    null,
                    new RuntimeException()
            ).test(schema, executeContext);
        } finally {
            canceller.join(5000);
        }

        assertTrue(CANCEL_OBSERVED.get(),
                "partition task should observe cancellation from ancestor context");
    }

    @Test
    public void testPartitionFailurePropagates() throws Exception {
        // a regular partition failure (unrelated to cancellation) should still propagate as an exception
        ExecuteContext executeContext = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TypeSupportTest.MyTable());
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        JavaFunctionUtils.registerTableFunction("default", "partition_fail_fun", PartitionFailFun.class);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase("cache table t2 as select * from myTable"),
                new SqlTestCase("cache table r2 as select int_type from myTable")
        );
        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }

        new SqlTestCase(
                "cache table r3 as call partition_fail_fun(t2) like r2 partition by t2 size 1",
                null,
                new RuntimeException()
        ).test(schema, executeContext);

        // Enabling partial results still reports an error when every partition fails.
        executeContext.setVariable("IGNORE_PARTITION_EXCEPTION", "true");
        String sql = "cache table r4 as call partition_fail_fun(t2) like r2 partition by t2 size 1";
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);
        BindableInterface bindable = new CompileManager().compileSql(
                sqlNode, schema, Consts.DEFAULT_SCHEMA_NAME, sql);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> bindable.bind(schema, executeContext)
        );
        Throwable allFailed = findCause(exception, "All 3 partition executions failed");
        assertNotNull(allFailed, "the failure should report that every partition failed");
        assertNotNull(allFailed.getCause(), "the first partition failure should be retained as the cause");
        assertEquals(2, allFailed.getSuppressed().length,
                "the remaining partition failures should be retained as suppressed exceptions");
    }

    private static Throwable findCause(Throwable failure, String expectedMessage) {
        Throwable current = failure;
        while (current != null) {
            if (expectedMessage.equals(current.getMessage())) {
                return current;
            }
            current = current.getCause();
        }
        return null;
    }

    public static class PartitionFailFun {
        public CacheTable evaluate(CacheTable input) {
            throw new RuntimeException("partition always fails");
        }
    }
}
