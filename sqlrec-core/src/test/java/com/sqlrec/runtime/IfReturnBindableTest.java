package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IfReturnBindableTest {

    private static final List<RelDataTypeField> INT_FIELDS = fields("id", SqlTypeName.INTEGER);
    private static final List<RelDataTypeField> BIGINT_FIELDS = fields("id", SqlTypeName.BIGINT);

    @Test
    void thenReturnWithoutElseIsAccepted() {
        IfBindable bindable = new IfBindable(
                condition(Boolean.TRUE),
                returning(INT_FIELDS, 1),
                null,
                false
        );

        assertTrue(bindable.containsReturn());
        assertSame(INT_FIELDS, bindable.getReturnDataFields());
    }

    @Test
    void bothReturnBranchesWithTheSameSchemaAreAccepted() {
        IfBindable bindable = new IfBindable(
                condition(Boolean.FALSE),
                returning(INT_FIELDS, 1),
                returning(INT_FIELDS, 2),
                false
        );

        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(2, result.single()[0]);
        assertSame(result, function.getFunctionReturnResult());
    }

    @Test
    void falseConditionWithoutElseDoesNotReturn() {
        IfBindable bindable = new IfBindable(
                condition(Boolean.FALSE),
                returning(INT_FIELDS, 1),
                null,
                false
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(0, result.count());
        assertFalse(function.hasReturnedFromFunction());
    }

    @Test
    void elseOnlyReturnIsRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.FALSE),
                value(INT_FIELDS, 1),
                returning(INT_FIELDS, 2),
                false
        ));

        assertReturnShapeError(error);
    }

    @Test
    void thenReturnWithNonReturningElseIsRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.TRUE),
                returning(INT_FIELDS, 1),
                value(INT_FIELDS, 2),
                false
        ));

        assertReturnShapeError(error);
    }

    @Test
    void emptyAndDataReturnBranchesAreRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.TRUE),
                new ReturnBindable((BindableInterface) null),
                returning(INT_FIELDS, 2),
                false
        ));

        assertTrue(error.getMessage().contains("compatible data fields"));
    }

    @Test
    void returnBranchesWithDifferentFieldCountAreRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.TRUE),
                returning(INT_FIELDS, 1),
                returning(List.of(
                        DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER),
                        DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR)
                ), 2),
                false
        ));

        assertTrue(error.getMessage().contains("field count not equal"));
    }

    @Test
    void returnBranchesWithDifferentFieldNamesAreRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.TRUE),
                returning(INT_FIELDS, 1),
                returning(fields("other", SqlTypeName.INTEGER), 2),
                false
        ));

        assertTrue(error.getMessage().contains("field name not equal"));
    }

    @Test
    void returnBranchesWithDifferentFieldTypesAreRejected() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> new IfBindable(
                condition(Boolean.TRUE),
                returning(INT_FIELDS, 1),
                returning(BIGINT_FIELDS, 2L),
                false
        ));

        assertTrue(error.getMessage().contains("field type not equal"));
    }

    @Test
    void timeinSuccessfulReturnCommitsTemporaryState() {
        AtomicBoolean elseExecuted = new AtomicBoolean(false);
        IfBindable bindable = new IfBindable(
                condition(1000L),
                returning(INT_FIELDS, 1),
                returning(INT_FIELDS, elseExecuted, 2),
                true
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        Enumerable<Object[]> result = bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(1, result.single()[0]);
        assertEquals(1, function.getFunctionReturnResult().single()[0]);
        assertFalse(elseExecuted.get());
    }

    @Test
    void nonPositiveTimeinExecutesReturnDirectly() {
        IfBindable bindable = new IfBindable(
                condition(0L),
                returning(INT_FIELDS, 1),
                returning(INT_FIELDS, 2),
                true
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(1, function.getFunctionReturnResult().single()[0]);
    }

    @Test
    void timeinTimeoutDiscardsLateThenReturnAndKeepsElseReturn() throws Exception {
        CountDownLatch thenFinished = new CountDownLatch(1);
        BindableInterface slowValue = new ValueBindable(INT_FIELDS, 1) {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                long deadline = System.currentTimeMillis() + 150;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(Math.max(1, deadline - System.currentTimeMillis()));
                    } catch (InterruptedException ignored) {
                        // Deliberately finish after timeout to verify isolation.
                    }
                }
                thenFinished.countDown();
                return super.bind(schema, context);
            }
        };
        IfBindable bindable = new IfBindable(
                condition(10L),
                new ReturnBindable(slowValue),
                returning(INT_FIELDS, 2),
                true
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(2, function.getFunctionReturnResult().single()[0]);
        assertTrue(thenFinished.await(2, TimeUnit.SECONDS));
        assertEquals(2, function.getFunctionReturnResult().single()[0]);
    }

    @Test
    void timeinExceptionDiscardsThenStateAndFallsBackToElseReturn() {
        BindableInterface failure = new ValueBindable(INT_FIELDS, 1) {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                throw new RuntimeException("expected failure");
            }
        };
        IfBindable bindable = new IfBindable(
                condition(1000L),
                new ReturnBindable(failure),
                returning(INT_FIELDS, 2),
                true
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(2, function.getFunctionReturnResult().single()[0]);
    }

    @Test
    void timeinExceptionAfterTemporaryReturnStillKeepsElseReturn() {
        BindableInterface partialReturnThenFailure = new ValueBindable(INT_FIELDS, 1) {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                Enumerable<Object[]> temporary = super.bind(schema, context);
                ((ExecuteContextImpl) context).returnFromFunction(temporary);
                throw new RuntimeException("failure after temporary return");
            }

            @Override
            public boolean containsReturn() {
                return true;
            }
        };
        IfBindable bindable = new IfBindable(
                condition(1000L),
                partialReturnThenFailure,
                returning(INT_FIELDS, 2),
                true
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(2, function.getFunctionReturnResult().single()[0]);
    }

    @Test
    void nullConditionSelectsElseReturn() {
        IfBindable bindable = new IfBindable(
                condition(null),
                returning(INT_FIELDS, 1),
                returning(INT_FIELDS, 2),
                false
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        bindable.bind(CalciteSchema.createRootSchema(false), function);

        assertEquals(2, function.getFunctionReturnResult().single()[0]);
    }

    @Test
    void emptyReturnsAreCompatibleInBothBranches() {
        IfBindable bindable = new IfBindable(
                condition(Boolean.TRUE),
                new ReturnBindable((BindableInterface) null),
                new ReturnBindable((BindableInterface) null),
                false
        );
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        assertNull(bindable.bind(CalciteSchema.createRootSchema(false), function));
        assertTrue(function.hasReturnedFromFunction());
        assertNull(function.getFunctionReturnResult());
    }

    private static ReturnBindable returning(List<RelDataTypeField> fields, Object value) {
        return new ReturnBindable(value(fields, value));
    }

    private static ReturnBindable returning(
            List<RelDataTypeField> fields,
            AtomicBoolean executed,
            Object value
    ) {
        return new ReturnBindable(new ValueBindable(fields, value) {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                executed.set(true);
                return super.bind(schema, context);
            }
        });
    }

    private static BindableInterface value(List<RelDataTypeField> fields, Object value) {
        return new ValueBindable(fields, value);
    }

    private static CalciteBindable condition(Object value) {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{value});
        return new CalciteBindable(
                new HashMap<>(),
                dataContext -> Linq4j.asEnumerable(rows),
                null, null, null, null, null
        );
    }

    private static List<RelDataTypeField> fields(String name, SqlTypeName type) {
        return List.of(DataTypeUtils.getRelDataTypeField(name, 0, type));
    }

    private static void assertReturnShapeError(RuntimeException error) {
        assertTrue(error.getMessage().contains(
                "IF with RETURN must either omit ELSE or return from both THEN and ELSE branches"
        ));
    }

    private static class ValueBindable extends BindableInterface {
        private final List<RelDataTypeField> fields;
        private final Object value;

        private ValueBindable(List<RelDataTypeField> fields, Object value) {
            this.fields = fields;
            this.value = value;
        }

        @Override
        public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
            return Linq4j.singletonEnumerable(new Object[]{value});
        }

        @Override
        public List<RelDataTypeField> getReturnDataFields() {
            return fields;
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
    }
}
