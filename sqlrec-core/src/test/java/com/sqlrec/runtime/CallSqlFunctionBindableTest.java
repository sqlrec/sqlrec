package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.schema.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CallSqlFunctionBindableTest {
    private boolean originalParallelism;

    @BeforeEach
    void setUp() {
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(true);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(ConcurrentCalciteSchema.createRootSchema());
        JavaFunctionUtils.setSkipHmsQuery(true);
    }

    @AfterEach
    void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @Test
    void sameNameArgumentKeepsItsProducerDependency() throws Exception {
        compile("same_name_child", "DEFINE INPUT TABLE input1(id integer)", "RETURN input1");
        SqlFunctionBindable parent = compile("same_name_parent",
                "CACHE TABLE input1 AS SELECT 7 AS id",
                "CACHE TABLE output_table AS CALL same_name_child(input1)",
                "RETURN output_table");

        assertEquals(Set.of("input1"), parent.getBindableList().get(1).getReadTables());
        assertEquals(Set.of(0), parent.getBindableDependency().get(1));
        assertEquals(7, execute(parent));
    }

    @Test
    void argumentMatchingAnotherParameterKeepsBothProducerDependencies() throws Exception {
        compile("cross_name_child", "DEFINE INPUT TABLE p(id integer)",
                "DEFINE INPUT TABLE q(id integer)", "RETURN p");
        SqlFunctionBindable parent = compile("cross_name_parent",
                "CACHE TABLE q AS SELECT 7 AS id",
                "CACHE TABLE r AS SELECT 9 AS id",
                "CACHE TABLE output_table AS CALL cross_name_child(q, r)",
                "RETURN output_table");

        assertEquals(Set.of("q", "r"), parent.getBindableList().get(2).getReadTables());
        assertEquals(Set.of(0, 1), parent.getBindableDependency().get(2));
        assertEquals(7, execute(parent));
    }

    @Test
    void repeatedArgumentMatchingAParameterKeepsItsDependency() throws Exception {
        compile("repeated_child", "DEFINE INPUT TABLE p(id integer)",
                "DEFINE INPUT TABLE q(id integer)", "RETURN q");
        SqlFunctionBindable parent = compile("repeated_parent",
                "CACHE TABLE p AS SELECT 7 AS id",
                "CACHE TABLE output_table AS CALL repeated_child(p, p)",
                "RETURN output_table");

        assertEquals(Set.of("p"), parent.getBindableList().get(1).getReadTables());
        assertEquals(Set.of(0), parent.getBindableDependency().get(1));
        assertEquals(7, execute(parent));
    }

    @Test
    void argumentAndParameterNamesAreNormalizedBeforeDependencyAnalysis() throws Exception {
        compile("mixed_case_child", "DEFINE INPUT TABLE Input1(id integer)", "RETURN input1");
        SqlFunctionBindable parent = compile("mixed_case_parent",
                "CACHE TABLE INPUT1 AS SELECT 7 AS id",
                "CACHE TABLE output_table AS CALL mixed_case_child(Input1)",
                "RETURN output_table");

        assertEquals(Set.of("input1"), parent.getBindableList().get(1).getReadTables());
        assertEquals(Set.of(0), parent.getBindableDependency().get(1));
        assertEquals(7, execute(parent));
    }

    @Test
    void normalizedCacheWritesStillReplaceMixedCaseFunctionInputs() throws Exception {
        for (boolean parallel : List.of(false, true)) {
            SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
            for (String write : List.of("CACHE TABLE Input1 AS SELECT 9 AS id",
                    "IF (SELECT FALSE) THEN (CACHE TABLE Input1 AS SELECT 8 AS id) "
                            + "ELSE (CACHE TABLE INPUT1 AS SELECT 9 AS id)")) {
                compile("overwrite_input_child", "DEFINE INPUT TABLE Input1(id integer)",
                        write, "RETURN SELECT * FROM Input1");
                for (String call : List.of("overwrite_input_child(seed_table)",
                        "GET_OR_DEFAULT('callee', 'overwrite_input_child')(seed_table) LIKE seed_table")) {
                    SqlFunctionBindable parent = compile("overwrite_input_parent",
                            "CACHE TABLE seed_table AS SELECT 7 AS id",
                            "RETURN CALL " + call);

                    assertEquals(9, execute(parent), "parallel=" + parallel + ", write=" + write + ", call=" + call);
                }
            }
        }
    }

    @Test
    void calleePublishesIntoItsOwnSchemaWithoutLockingTheCallerSchema() throws Exception {
        SqlFunctionBindable child = compile("isolated_child",
                "CACHE TABLE output_table AS SELECT 9 AS id", "RETURN output_table");
        CallSqlFunctionBindable call = new CallSqlFunctionBindable("isolated_child", List.of(), child);
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        CacheTable previous = new CacheTable("output_table",
                Linq4j.singletonEnumerable(new Object[]{7}), child.getReturnDataFields());
        schema.add("output_table", previous);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);

        synchronized (schema) {
            assertEquals(9, CompletableFuture.supplyAsync(() -> call.bind(schema, context).single()[0])
                    .get(5, TimeUnit.SECONDS));
        }
        assertSame(previous, schema.getTable("output_table", false).getTable());
        assertFalse(context.isCancelled());
    }

    @Test
    void parallelCallWaitsUntilItsInputProducerFinishes() throws Exception {
        SqlFunctionBindable child = compile("waiting_child",
                "DEFINE INPUT TABLE input1(id integer)", "RETURN input1");
        List<RelDataTypeField> fields = child.getReturnDataFields();
        CountDownLatch producerStarted = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        CountDownLatch callStarted = new CountDownLatch(1);
        BindableInterface producer = new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                producerStarted.countDown();
                try {
                    if (!releaseProducer.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("producer was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return Linq4j.asEnumerable(List.<Object[]>of(new Object[]{7}));
            }

            @Override public List<RelDataTypeField> getReturnDataFields() { return fields; }
            @Override public boolean isParallelizable() { return true; }
            @Override public Set<String> getReadTables() { return Set.of(); }
            @Override public Set<String> getWriteTables() { return Set.of(); }
        };
        CallSqlFunctionBindable call = new CallSqlFunctionBindable("waiting_child", List.of("input1"), child) {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                callStarted.countDown();
                return super.bind(schema, context);
            }
        };
        // Use a CACHE call rather than RETURN as the consumer: RETURN itself is a barrier.
        SqlFunctionBindable parent = new SqlFunctionBindable(List.of(), List.of(
                new CacheTableBindable("input1", producer), new CacheTableBindable("output_table", call),
                new ReturnBindable("output_table", fields)), fields);
        parent.setFunName("waiting_parent");
        parent.init();
        assertEquals(Set.of(0), parent.getBindableDependency().get(1));

        CompletableFuture<Object> result = CompletableFuture.supplyAsync(() -> execute(parent));
        try {
            assertTrue(producerStarted.await(5, TimeUnit.SECONDS));
            assertEquals(1, callStarted.getCount());
            assertFalse(result.isDone());
        } finally {
            releaseProducer.countDown();
        }
        assertEquals(7, result.get(5, TimeUnit.SECONDS));
        assertEquals(0, callStarted.getCount());
    }

    private SqlFunctionBindable compile(String name, String... statements) throws Exception {
        List<String> sql = new ArrayList<>();
        sql.add("CREATE SQL FUNCTION " + name);
        sql.addAll(List.of(statements));
        return new CompileManager().compileSqlFunction(name, sql);
    }

    private Object execute(SqlFunctionBindable function) {
        CalciteSchema schema = ConcurrentCalciteSchema.createRootSchema();
        return function.bind(schema, new ExecuteContextImpl(schema))
                .toList().get(0)[0];
    }
}
