package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ReturnBindableTest {

    private static final List<RelDataTypeField> FIELDS = List.of(
            DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER)
    );

    @Test
    void tableReturnScansTheCacheAndCompletesTheFunction() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("result_table", new CacheTable(
                "result_table",
                Linq4j.asEnumerable(new Object[][]{{1}, {2}}),
                FIELDS
        ));
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        ReturnBindable bindable = new ReturnBindable("result_table", FIELDS);

        Enumerable<Object[]> result = bindable.bind(schema, function);

        assertEquals(List.of(1, 2), result.select(row -> (Integer) row[0]).toList());
        assertSame(result, function.getFunctionReturnResult());
        assertEquals(Set.of("result_table"), bindable.getReadTables());
        assertTrue(bindable.getWriteTables().isEmpty());
        assertSame(FIELDS, bindable.getReturnDataFields());
        assertTrue(bindable.containsReturn());
        assertTrue(bindable.isParallelizable());
        assertFalse(bindable.isTimeoutAble(schema, function));
    }

    @Test
    void emptyReturnCompletesTheFunctionWithNull() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        ReturnBindable bindable = new ReturnBindable((BindableInterface) null);

        assertNull(bindable.bind(CalciteSchema.createRootSchema(false), function));
        assertTrue(function.hasReturnedFromFunction());
        assertNull(function.getFunctionReturnResult());
        assertNull(bindable.getReturnDataFields());
        assertTrue(bindable.getReadTables().isEmpty());
    }

    @Test
    void delegatedReturnExecutesOnceAndForwardsMetadata() {
        AtomicInteger calls = new AtomicInteger();
        Enumerable<Object[]> expected = Linq4j.singletonEnumerable(new Object[]{7});
        BindableInterface delegate = new MetadataBindable(calls, expected);
        ReturnBindable bindable = new ReturnBindable(delegate);
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        Enumerable<Object[]> actual = bindable.bind(schema, function);

        assertSame(expected, actual);
        assertSame(expected, function.getFunctionReturnResult());
        assertEquals(1, calls.get());
        assertSame(FIELDS, bindable.getReturnDataFields());
        assertFalse(bindable.isParallelizable());
        assertTrue(bindable.isTimeoutAble(schema, function));
        assertEquals(Set.of("source"), bindable.getReadTables());
        assertEquals(Set.of("written"), bindable.getWriteTables());
        assertEquals(Set.of("java_fun"), bindable.getDependencyJavaFuncName());
        assertEquals(Set.of("sql_fun"), bindable.getDependencySqlFuncName());
        assertEquals(Map.of("nested_fun", "sql_fun->nested_fun"), bindable.getAllDependSqlFunctionMap());
        assertTrue(bindable.isUnionSql());
        assertEquals("logical", bindable.getLogicalPlan());
        assertEquals("physical", bindable.getPhysicalPlan());
        assertEquals("java", bindable.getJavaExpression());
    }

    @Test
    void bindRejectsNonEngineContext() {
        ReturnBindable bindable = new ReturnBindable((BindableInterface) null);

        RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> bindable.bind(CalciteSchema.createRootSchema(false), mock(ExecuteContext.class))
        );

        assertEquals("return statement context must be ExecuteContextImpl", error.getMessage());
    }

    @Test
    void bindRejectsEngineContextWithoutFunctionFrame() {
        ReturnBindable bindable = new ReturnBindable((BindableInterface) null);

        RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> bindable.bind(CalciteSchema.createRootSchema(false), new ExecuteContextImpl())
        );

        assertTrue(error.getMessage().contains("inside a SQL function"));
    }

    private static class MetadataBindable extends BindableInterface {
        private final AtomicInteger calls;
        private final Enumerable<Object[]> result;

        private MetadataBindable(AtomicInteger calls, Enumerable<Object[]> result) {
            this.calls = calls;
            this.result = result;
        }

        @Override
        public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
            calls.incrementAndGet();
            return result;
        }

        @Override
        public List<RelDataTypeField> getReturnDataFields() {
            return FIELDS;
        }

        @Override
        public boolean isParallelizable() {
            return false;
        }

        @Override
        public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
            return true;
        }

        @Override
        public Set<String> getReadTables() {
            return Set.of("source");
        }

        @Override
        public Set<String> getWriteTables() {
            return Set.of("written");
        }

        @Override
        public Set<String> getDependencyJavaFuncName() {
            return Set.of("java_fun");
        }

        @Override
        public Set<String> getDependencySqlFuncName() {
            return Set.of("sql_fun");
        }

        @Override
        public Map<String, String> getAllDependSqlFunctionMap() {
            return Map.of("nested_fun", "sql_fun->nested_fun");
        }

        @Override
        public boolean isUnionSql() {
            return true;
        }

        @Override
        public String getLogicalPlan() {
            return "logical";
        }

        @Override
        public String getPhysicalPlan() {
            return "physical";
        }

        @Override
        public String getJavaExpression() {
            return "java";
        }
    }
}
