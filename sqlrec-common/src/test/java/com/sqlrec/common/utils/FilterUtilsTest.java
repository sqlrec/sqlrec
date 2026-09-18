package com.sqlrec.common.utils;

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
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterUtilsTest {
    private RexBuilder rexBuilder;
    private RelDataTypeFactory types;

    @BeforeEach
    void setUp() {
        types = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(types);
    }

    @Test
    void extractsPrimaryKeyWithEitherOperandOrder() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, integer(7), input(0, SqlTypeName.INTEGER));

        assertEquals(7L, FilterUtils.extractPrimaryKeyValue(
                Collections.singletonList(condition), 0));
        assertEquals(Collections.singletonList(condition), FilterUtils.getPrimaryKeyFilters(
                Collections.singletonList(condition), 0));
    }

    @Test
    void rejectsNonPrimaryKeyPredicates() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                input(0, SqlTypeName.INTEGER), integer(7));

        assertNull(FilterUtils.extractPrimaryKeyValue(Collections.singletonList(condition), 0));
        assertTrue(FilterUtils.getPrimaryKeyFilters(
                Collections.singletonList(condition), 0).isEmpty());
    }

    private RexInputRef input(int index, SqlTypeName type) {
        return rexBuilder.makeInputRef(types.createSqlType(type), index);
    }

    private RexNode integer(int value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
    }
}
