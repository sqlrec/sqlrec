package com.sqlrec.utils;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.SqlRecDataContext;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.runtime.SqlRecDataContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KvJoinUtilsTest {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType rightRowType;
    private boolean originalIgnoreJoinQueryException;

    @BeforeEach
    public void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        rightRowType = typeFactory.builder()
                .add("right_id", SqlTypeName.INTEGER)
                .add("right_name", SqlTypeName.VARCHAR)
                .build();
        originalIgnoreJoinQueryException = SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.getDefaultValue();
    }

    @AfterEach
    public void tearDown() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(originalIgnoreJoinQueryException);
    }

    @Test
    public void testScalarRowsKeepStringKeyMatchingAndSnakeOrder() {
        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);
        Object[] first = {1, "first"};
        when(rightTable.getByPrimaryKey(Set.of(1, 2, 9))).thenReturn(Map.of(
                "1", List.of(first, new Object[]{1, "second"}),
                "2", Collections.singletonList(new Object[]{2, "other"})));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        List<Object[]> rows = KvJoinUtils.kvJoin(Linq4j.asEnumerable(Arrays.asList(1, 2, null, 9)),
                rightTable, condition, JoinRelType.LEFT).toList();

        assertEquals(5, rows.size());
        assertArrayEquals(new Object[]{1, 1, "first"}, rows.get(0));
        assertArrayEquals(new Object[]{2, 2, "other"}, rows.get(1));
        assertArrayEquals(new Object[]{null, null, null}, rows.get(2));
        assertArrayEquals(new Object[]{9, null, null}, rows.get(3));
        assertArrayEquals(new Object[]{1, 1, "second"}, rows.get(4));
        rows.get(0)[2] = "changed";
        assertEquals("first", first[1]);
        verify(rightTable).getByPrimaryKey(Set.of(1, 2, 9));
        verify(rightTable, never()).scan(any(), any());
    }

    @Test
    public void testEmptyLeftRowsSkipConditionAndRightTable() {
        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        assertEquals(0, KvJoinUtils.kvJoin(Linq4j.emptyEnumerable(), rightTable, null, JoinRelType.INNER).count());
        verifyNoInteractions(rightTable);
    }

    @Test
    @SilenceLoggers(KvJoinUtils.class)
    public void testKvJoinIgnoreQueryExceptionWhenEnabled() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);

        Object[] leftRow1 = new Object[]{1, "Alice"};
        Object[] leftRow2 = new Object[]{2, "Bob"};
        Enumerable left = Linq4j.asEnumerable(Arrays.asList(leftRow1, leftRow2));

        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);
        when(rightTable.scan(any(), any())).thenThrow(new RuntimeException("scan failed"));

        // Join condition: left.left_name = right.right_name => RexInputRef(1) = RexInputRef(3)
        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        Enumerable result = KvJoinUtils.kvJoin(left, rightTable, condition, JoinRelType.INNER);

        assertNotNull(result);
        assertEquals(0, result.count());
        verify(rightTable, atLeastOnce()).scan(any(), any());
    }

    @Test
    public void testKvJoinRethrowQueryExceptionWhenDisabled() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(false);

        Object[] leftRow1 = new Object[]{1, "Alice"};
        Enumerable left = Linq4j.asEnumerable(Collections.singletonList(leftRow1));

        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);
        when(rightTable.scan(any(), any())).thenThrow(new RuntimeException("scan failed"));

        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> KvJoinUtils.kvJoin(left, rightTable, condition, JoinRelType.INNER)
        );
        assertEquals("scan failed", exception.getMessage());
    }

    @Test
    @SilenceLoggers(KvJoinUtils.class)
    public void testKvJoinPartialFailureWhenIgnoreEnabled() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);

        Object[] leftRow1 = new Object[]{1, "Alice"};
        Object[] leftRow2 = new Object[]{2, "Bob"};
        Enumerable left = Linq4j.asEnumerable(Arrays.asList(leftRow1, leftRow2));

        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);

        Object[] rightRow = new Object[]{200, "match"};
        when(rightTable.scan(any(), any()))
                .thenThrow(new RuntimeException("scan failed for first key"))
                .thenReturn(Linq4j.asEnumerable(Collections.singletonList(rightRow)));

        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        Enumerable result = KvJoinUtils.kvJoin(left, rightTable, condition, JoinRelType.INNER);

        assertNotNull(result);
        // One key's scan failed, the other succeeded; exactly 1 result row
        assertEquals(1, result.count());
        Object[] row = (Object[]) result.first();
        // Right table columns are always the returned row regardless of which key succeeded
        assertEquals(200, row[2]);
        assertEquals("match", row[3]);
    }

    @Test
    public void testExecutionContextOverridesDefault() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);
        ExecuteContextImpl executeContext = new ExecuteContextImpl();
        executeContext.setVariable("IGNORE_JOIN_QUERY_EXCEPTION", "false");
        SqlRecDataContext dataContext = new SqlRecDataContextImpl(
                Collections.emptyMap(), CalciteSchema.createRootSchema(false), executeContext);

        Enumerable left = Linq4j.asEnumerable(Collections.singletonList(new Object[]{1, "Alice"}));
        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);
        when(rightTable.scan(any(), any())).thenThrow(new RuntimeException("scan failed"));

        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                KvJoinUtils.kvJoin(left, rightTable, condition, JoinRelType.INNER, dataContext));
        assertEquals("scan failed", exception.getMessage());
    }

    @Test
    public void testLeftJoinKeepsLeftRowWithNullJoinKey() {
        Object[] leftNullKey = new Object[]{null, "Alice"};
        Enumerable left = Linq4j.asEnumerable(Collections.singletonList(leftNullKey));

        SqlRecKvTable rightTable = mock(SqlRecKvTable.class);
        when(rightTable.getRowType(any())).thenReturn(rightRowType);
        when(rightTable.getPrimaryKeyIndex()).thenReturn(0);
        when(rightTable.getByPrimaryKey(any())).thenReturn(Collections.emptyMap());

        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        Enumerable result = KvJoinUtils.kvJoin(left, rightTable, condition, JoinRelType.LEFT);

        assertEquals(1, result.count());
        Object[] row = (Object[]) result.first();
        assertNull(row[0]);
        assertEquals("Alice", row[1]);
        assertNull(row[2]);
        assertNull(row[3]);
    }

    @Test
    public void testCopyValuesWithBothValues() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 2);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(100, result[2]);
        assertEquals("Engineer", result[3]);
    }

    @Test
    public void testCopyValuesWithNullRightValue() {
        Object[] leftValue = new Object[]{1, "Alice"};

        Object[] result = KvJoinUtils.copyValues(leftValue, null, 2, 2);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertNull(result[2]);
        assertNull(result[3]);
    }

    @Test
    public void testCopyValuesWithEmptyLeftValue() {
        Object[] leftValue = new Object[]{};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 0, 2);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(100, result[0]);
        assertEquals("Engineer", result[1]);
    }

    @Test
    public void testCopyValuesWithEmptyRightValue() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 0);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
    }

    @Test
    public void testCopyValuesWithDifferentSizes() {
        Object[] leftValue = new Object[]{1, "Alice", 30};
        Object[] rightValue = new Object[]{100};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 3, 1);

        assertNotNull(result);
        assertEquals(4, result.length);
        assertEquals(1, result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(30, result[2]);
        assertEquals(100, result[3]);
    }

    @Test
    public void testCopyValuesWithNullElements() {
        Object[] leftValue = new Object[]{1, null, "Alice"};
        Object[] rightValue = new Object[]{null, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 3, 2);

        assertNotNull(result);
        assertEquals(5, result.length);
        assertEquals(1, result[0]);
        assertNull(result[1]);
        assertEquals("Alice", result[2]);
        assertNull(result[3]);
        assertEquals("Engineer", result[4]);
    }

    @Test
    public void testCopyValuesPreservesOriginalArrays() {
        Object[] leftValue = new Object[]{1, "Alice"};
        Object[] rightValue = new Object[]{100, "Engineer"};

        Object[] result = KvJoinUtils.copyValues(leftValue, rightValue, 2, 2);

        result[0] = 999;
        result[3] = "Modified";

        assertEquals(1, leftValue[0]);
        assertEquals(100, rightValue[0]);
    }
    @Test
    @SilenceLoggers({SqlRecKvTable.class, KvJoinUtils.class})
    void primaryKeyBulkFailureUsesCacheWithoutRetryingBackend() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);
        SqlRecKvTable table = mock(SqlRecKvTable.class, CALLS_REAL_METHODS);
        when(table.getTableName()).thenReturn("cached-right");
        when(table.getRowType(any())).thenReturn(rightRowType);
        when(table.getPrimaryKeyIndex()).thenReturn(0);
        table.initCache(100, 60);
        when(table.getByPrimaryKeyImpl(Set.of(2))).thenReturn(Map.of(2,
                Collections.singletonList(new Object[]{2, "match"})));
        table.getByPrimaryKey(Set.of(2));
        when(table.getByPrimaryKeyImpl(Set.of(1))).thenThrow(new RuntimeException("backend failed"));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2));
        List<Object[]> result = KvJoinUtils.kvJoin(Linq4j.asEnumerable(Arrays.asList(
                new Object[]{1, "Alice"}, new Object[]{2, "Bob"})), table, condition, JoinRelType.LEFT).toList();
        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(row -> Arrays.equals(row, new Object[]{1, "Alice", null, null})));
        assertTrue(result.stream().anyMatch(row -> Arrays.equals(row, new Object[]{2, "Bob", 2, "match"})));
        verify(table).getByPrimaryKey(Set.of(1, 2));
        verify(table).getCachedByPrimaryKey(Set.of(1, 2));
        verify(table).getByPrimaryKeyImpl(Set.of(1));
        verify(table).getByPrimaryKeyImpl(Set.of(2));
        verify(table, times(2)).getByPrimaryKeyImpl(any());
    }

    @Test
    @SilenceLoggers({SqlRecKvTable.class, KvJoinUtils.class})
    void primaryKeyFailureWithoutCacheKeepsLeftRowsAndReturnsNoInnerMatches() {
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);
        SqlRecKvTable table = mock(SqlRecKvTable.class, CALLS_REAL_METHODS);
        when(table.getTableName()).thenReturn("uncached-right");
        when(table.getRowType(any())).thenReturn(rightRowType);
        when(table.getPrimaryKeyIndex()).thenReturn(0);
        when(table.getByPrimaryKeyImpl(any())).thenThrow(new RuntimeException("backend failed"));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        List<Object[]> result = KvJoinUtils.kvJoin(Linq4j.asEnumerable(List.of(1)),
                table, condition, JoinRelType.LEFT).toList();
        assertEquals(1, result.size());
        assertArrayEquals(new Object[]{1, null, null}, result.get(0));
        assertEquals(0, KvJoinUtils.kvJoin(Linq4j.asEnumerable(List.of(1)),
                table, condition, JoinRelType.INNER).count());
        verify(table, times(2)).getByPrimaryKeyImpl(Set.of(1));
    }

    @Test
    void primaryKeyFailurePropagatesWhenDisabledAndCancellationIsNeverIgnored() {
        SqlRecKvTable table = mock(SqlRecKvTable.class);
        when(table.getRowType(any())).thenReturn(rightRowType);
        when(table.getPrimaryKeyIndex()).thenReturn(0);
        RuntimeException failure = new RuntimeException("backend failed");
        when(table.getByPrimaryKey(any())).thenThrow(failure);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(false);
        assertSame(failure, assertThrows(RuntimeException.class, () ->
                KvJoinUtils.kvJoin(Linq4j.asEnumerable(List.of(1)), table, condition, JoinRelType.LEFT)));
        SqlRecConfigs.IGNORE_JOIN_QUERY_EXCEPTION.setDefaultValue(true);
        java.util.concurrent.CancellationException cancelled = new java.util.concurrent.CancellationException();
        doThrow(cancelled).when(table).getByPrimaryKey(any());
        assertSame(cancelled, assertThrows(java.util.concurrent.CancellationException.class, () ->
                KvJoinUtils.kvJoin(Linq4j.asEnumerable(List.of(1)), table, condition, JoinRelType.LEFT)));
        verify(table, times(2)).getByPrimaryKey(any());
        verify(table, never()).getCachedByPrimaryKey(any());
    }

}
