package com.sqlrec.connectors.milvus.filter;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MilvusFilterBuilderTest {
    private RexBuilder rexBuilder;
    private RelDataTypeFactory types;
    private List<FieldSchema> fields;

    @BeforeEach
    void setUp() {
        types = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(types);
        fields = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("category", "VARCHAR"));
    }

    @Test
    void buildsScanFilterAndEscapesStrings() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                input(1, SqlTypeName.VARCHAR),
                rexBuilder.makeLiteral("a\\\"b"));

        assertEquals("category == \"a\\\\\\\"b\"", MilvusFilterBuilder.buildScanFilter(
                Collections.singletonList(condition), fields));
    }

    @Test
    void refusesPartialCompoundScanFilter() {
        RexNode supported = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                input(0, SqlTypeName.INTEGER), integer(1));
        RexNode unsupported = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
                input(1, SqlTypeName.VARCHAR));

        assertNull(MilvusFilterBuilder.buildScanFilter(Collections.singletonList(
                rexBuilder.makeCall(SqlStdOperatorTable.AND, supported, unsupported)), fields));
    }

    @Test
    void validatesMilvusFieldNames() {
        List<FieldSchema> unsafe = Collections.singletonList(
                new FieldSchema("id or true", "INTEGER"));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                input(0, SqlTypeName.INTEGER), integer(1));

        assertThrows(IllegalArgumentException.class, () ->
                MilvusFilterBuilder.buildScanFilter(Collections.singletonList(condition), unsafe));
    }

    @Test
    void buildsCorrelatedJoinFilterAndReversesOperator() {
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                input(0, SqlTypeName.INTEGER), input(1, SqlTypeName.INTEGER));

        assertTrue(MilvusFilterBuilder.supportsJoinFilter(condition, 1, 1));
        assertEquals("id > 7", MilvusFilterBuilder.buildJoinFilter(
                condition, new Object[]{7}, Collections.singletonList("id")));
    }

    @Test
    void supportsRightFieldComparedWithLiteral() {
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                input(1, SqlTypeName.VARCHAR), rexBuilder.makeLiteral("book"));

        assertTrue(MilvusFilterBuilder.supportsJoinFilter(condition, 1, 1));
        assertEquals("category == \"book\"", MilvusFilterBuilder.buildJoinFilter(
                condition, new Object[]{42}, Collections.singletonList("category")));
    }

    @Test
    void rejectsLeftOnlyAndComputedJoinPredicates() {
        RexNode leftOnly = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                input(0, SqlTypeName.INTEGER), integer(1));
        RexNode computed = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                        input(1, SqlTypeName.INTEGER), integer(1)), integer(2));

        assertFalse(MilvusFilterBuilder.supportsJoinFilter(leftOnly, 1, 1));
        assertFalse(MilvusFilterBuilder.supportsJoinFilter(computed, 1, 1));
    }

    private RexInputRef input(int index, SqlTypeName type) {
        return rexBuilder.makeInputRef(types.createSqlType(type), index);
    }

    private RexNode integer(int value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
    }
}
