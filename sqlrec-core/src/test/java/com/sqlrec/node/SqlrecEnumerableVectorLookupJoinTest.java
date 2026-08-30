package com.sqlrec.node;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlrecEnumerableVectorLookupJoinTest {

    @Test
    public void keepsBothRelationalInputsInThePlan() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(typeFactory));
        RelDataType leftType = typeFactory.builder()
                .add("user_id", SqlTypeName.INTEGER)
                .add("embedding", SqlTypeName.ANY)
                .build();
        RelDataType rightType = typeFactory.builder()
                .add("item_id", SqlTypeName.INTEGER)
                .add("embedding", SqlTypeName.ANY)
                .build();
        RelNode left = input(cluster, leftType);
        RelNode right = input(cluster, rightType);

        SqlrecEnumerableVectorLookupJoin join =
                SqlrecEnumerableVectorLookupJoin.create(
                        left,
                        right,
                        null,
                        1,
                        "embedding",
                        10,
                        typeFactory.createSqlType(SqlTypeName.DOUBLE));

        assertEquals(2, join.getInputs().size());
        assertSame(left, join.getLeft());
        assertSame(right, join.getRight());
        assertEquals(5, join.getRowType().getFieldCount());
        assertEquals("$vector_score", join.getRowType().getFieldNames().get(4));
    }

    @Test
    public void copyKeepsExactlyTwoInputs() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(typeFactory));
        RelDataType rowType = typeFactory.builder()
                .add("embedding", SqlTypeName.ANY)
                .build();
        RelNode left = input(cluster, rowType);
        RelNode right = input(cluster, rowType);
        SqlrecEnumerableVectorLookupJoin join =
                SqlrecEnumerableVectorLookupJoin.create(
                        left,
                        right,
                        null,
                        0,
                        "embedding",
                        1,
                        typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RelNode newLeft = input(cluster, rowType);
        RelNode newRight = input(cluster, rowType);

        SqlrecEnumerableVectorLookupJoin copied =
                (SqlrecEnumerableVectorLookupJoin) join.copy(
                        join.getTraitSet(), Arrays.asList(newLeft, newRight));

        assertSame(newLeft, copied.getLeft());
        assertSame(newRight, copied.getRight());
        assertThrows(
                IllegalArgumentException.class,
                () -> join.copy(join.getTraitSet(), Collections.singletonList(newLeft)));
    }

    private RelNode input(RelOptCluster cluster, RelDataType rowType) {
        RelNode input = mock(RelNode.class);
        when(input.getCluster()).thenReturn(cluster);
        when(input.getTraitSet()).thenReturn(
                cluster.traitSetOf(EnumerableConvention.INSTANCE));
        when(input.getRowType()).thenReturn(rowType);
        return input;
    }
}
