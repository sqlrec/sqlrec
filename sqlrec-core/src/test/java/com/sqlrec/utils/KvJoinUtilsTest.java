package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.SqlRecKvTable;
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
}
