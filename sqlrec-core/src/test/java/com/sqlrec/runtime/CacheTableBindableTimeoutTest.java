package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class CacheTableBindableTimeoutTest {

    @Test
    public void testExecuteWithoutTimeout() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "0");

        List<Object[]> testData = new ArrayList<>();
        testData.add(new Object[]{"value1", 1});
        testData.add(new Object[]{"value2", 2});

        BindableInterface testBindable = createTestBindable(testData, true);
        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", testBindable, "SELECT * FROM test"
        );

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        Enumerable<Object[]> result = cacheTableBindable.bind(schema, context);

        assertNotNull(result);
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals(1, resultList.size());
        assertEquals("test_table", resultList.get(0)[0]);
        assertEquals(2L, resultList.get(0)[1]);
    }

    @Test
    public void testExecuteWithTimeoutSuccess() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "5000");

        List<Object[]> testData = new ArrayList<>();
        testData.add(new Object[]{"value1", 1});

        BindableInterface testBindable = createTestBindable(testData, true);
        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", testBindable, "SELECT * FROM test"
        );

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        Enumerable<Object[]> result = cacheTableBindable.bind(schema, context);

        assertNotNull(result);
    }

    @Test
    public void testExecuteWithTimeout() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");

        AtomicBoolean taskStarted = new AtomicBoolean(false);
        AtomicBoolean taskInterrupted = new AtomicBoolean(false);

        BindableInterface slowBindable = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                taskStarted.set(true);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    taskInterrupted.set(true);
                    throw new RuntimeException("Task interrupted", e);
                }
                List<Object[]> data = new ArrayList<>();
                data.add(new Object[]{"result"});
                return Linq4j.asEnumerable(data);
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return true;
            }
        };

        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", slowBindable, "SELECT * FROM test"
        );

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        long startTime = System.currentTimeMillis();
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            cacheTableBindable.bind(schema, context);
        });
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(duration < 5000, "Should timeout quickly, but took " + duration + "ms");
        assertTrue(exception.getMessage().contains("timeout") || 
                  exception.getMessage().contains("Task execution timeout"),
                  "Exception should indicate timeout, but was: " + exception.getMessage());
        assertTrue(taskStarted.get(), "Task should have started");
    }

    @Test
    public void testTimeoutNotAppliedWhenNotTimeoutAble() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");

        BindableInterface notTimeoutAbleBindable = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                List<Object[]> data = new ArrayList<>();
                data.add(new Object[]{"result"});
                return Linq4j.asEnumerable(data);
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return false;
            }
        };

        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", notTimeoutAbleBindable, "SELECT * FROM test"
        );

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        long startTime = System.currentTimeMillis();
        Enumerable<Object[]> result = cacheTableBindable.bind(schema, context);
        long duration = System.currentTimeMillis() - startTime;

        assertNotNull(result);
        assertTrue(duration >= 200, "Should complete without timeout, took " + duration + "ms");
    }

    @Test
    public void testIgnoreExceptionWithTimeout() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");

        BindableInterface timeoutBindable = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Task interrupted", e);
                }
                return Linq4j.emptyEnumerable();
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return true;
            }
        };

        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", timeoutBindable, "SELECT * FROM test"
        );
        cacheTableBindable.setIgnoreException(true);

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        Enumerable<Object[]> result = cacheTableBindable.bind(schema, context);

        assertNotNull(result);
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals(1, resultList.size());
        assertEquals("test_table", resultList.get(0)[0]);
        assertEquals(0L, resultList.get(0)[1]);
    }

    @Test
    public void testExceptionHandlingWithoutTimeout() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "0");

        BindableInterface exceptionBindable = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw new RuntimeException("Test exception");
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }
        };

        CacheTableBindable cacheTableBindable = new CacheTableBindable(
                "test_table", exceptionBindable, "SELECT * FROM test"
        );

        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            cacheTableBindable.bind(schema, context);
        });

        assertEquals("Test exception", exception.getMessage());
    }

    @Test
    public void testMultipleTimeoutsInSequence() {
        ExecuteContext context = new ExecuteContextImpl();
        context.setVariable(SqlRecConfigs.NODE_EXEC_TIMEOUT.getKey(), "100");

        AtomicInteger executionCount = new AtomicInteger(0);

        BindableInterface quickBindable = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executionCount.incrementAndGet();
                List<Object[]> data = new ArrayList<>();
                data.add(new Object[]{"result"});
                return Linq4j.asEnumerable(data);
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return true;
            }
        };

        for (int i = 0; i < 3; i++) {
            CacheTableBindable cacheTableBindable = new CacheTableBindable(
                    "test_table_" + i, quickBindable, "SELECT * FROM test"
            );

            CalciteSchema schema = CalciteSchema.createRootSchema(false);

            Enumerable<Object[]> result = cacheTableBindable.bind(schema, context);
            assertNotNull(result);
        }

        assertEquals(3, executionCount.get(), "All three executions should complete");
    }

    private BindableInterface createTestBindable(List<Object[]> data, boolean timeoutAble) {
        return new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                return Linq4j.asEnumerable(data);
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
                        DataTypeUtils.getRelDataTypeField("col2", 1, org.apache.calcite.sql.type.SqlTypeName.INTEGER)
                );
            }

            @Override
            public boolean isParallelizable() {
                return true;
            }

            @Override
            public Set<String> getReadTables() {
                return Set.of();
            }

            @Override
            public Set<String> getWriteTables() {
                return Set.of();
            }

            @Override
            public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
                return timeoutAble;
            }
        };
    }
}
