package com.sqlrec.utils;

import com.sqlrec.common.schema.VectorSearchable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VectorJoinPlanExtractorTest {
    private RexBuilder rexBuilder;
    private RelDataType joinedRowType;

    @BeforeEach
    public void setUp() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        joinedRowType = typeFactory.builder()
                .add("user_id", SqlTypeName.INTEGER)
                .add("preferred_category", SqlTypeName.VARCHAR)
                .add("item_id", SqlTypeName.INTEGER)
                .add("category", SqlTypeName.VARCHAR)
                .build();
    }

    @Test
    public void keepsLeftOnlyConditionOnLeftInput() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                input(0),
                integer(10));

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertTrue(split.isSupported());
        assertNotNull(split.getLeftFilter());
        assertNull(split.getPushedFilter());
    }

    @Test
    public void pushesRightOnlyConditionToMilvus() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                input(3),
                rexBuilder.makeLiteral("book"));

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertTrue(split.isSupported());
        assertNull(split.getLeftFilter());
        assertNotNull(split.getPushedFilter());
    }

    @Test
    public void pushesCorrelatedConditionToMilvus() {
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                input(1),
                input(3));

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertTrue(split.isSupported());
        assertNull(split.getLeftFilter());
        assertNotNull(split.getPushedFilter());
    }

    @Test
    public void splitsIndependentLeftAndRightConjunctions() {
        RexNode leftCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, input(0), integer(10));
        RexNode rightCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("book"));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.AND, leftCondition, rightCondition);

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertTrue(split.isSupported());
        assertNotNull(split.getLeftFilter());
        assertNotNull(split.getPushedFilter());
    }

    @Test
    public void rejectsUnsupportedRightCondition() {
        RexNode itemIdPlusOne = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS, input(2), integer(1));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, itemIdPlusOne, integer(10));

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertFalse(split.isSupported());
    }

    @Test
    public void rejectsWholeOrWhenOneBranchCannotBePushed() {
        RexNode supported = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("book"));
        RexNode itemIdPlusOne = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS, input(2), integer(1));
        RexNode unsupported = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, itemIdPlusOne, integer(10));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.OR, supported, unsupported);

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertFalse(split.isSupported());
    }

    @Test
    public void pushesWholeOrWhenEveryBranchIsSupported() {
        RexNode first = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("book"));
        RexNode second = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("movie"));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.OR, first, second);

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertTrue(split.isSupported());
        assertNull(split.getLeftFilter());
        assertNotNull(split.getPushedFilter());
    }

    @Test
    public void rejectsAllPushdownWhenOneAndTermIsUnsupported() {
        RexNode supported = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("book"));
        RexNode itemIdPlusOne = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS, input(2), integer(1));
        RexNode unsupported = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, itemIdPlusOne, integer(10));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.AND, supported, unsupported);

        VectorJoinPlanExtractor.FilterSplit split = split(condition);

        assertFalse(split.isSupported());
    }

    @Test
    public void rejectsOrThatMixesLeftAndRightConditions() {
        RexNode leftCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, input(0), integer(10));
        RexNode rightCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, input(3), rexBuilder.makeLiteral("book"));

        VectorJoinPlanExtractor.FilterSplit split = split(rexBuilder.makeCall(
                SqlStdOperatorTable.OR, leftCondition, rightCondition));

        assertFalse(split.isSupported());
    }

    @Test
    public void rejectsNonInnerAndNonTrueJoins() {
        LogicalJoin outerJoin = mock(LogicalJoin.class);
        when(outerJoin.getJoinType()).thenReturn(JoinRelType.LEFT);
        assertFalse(VectorJoinPlanExtractor.extract(
                mock(LogicalSort.class), mock(LogicalProject.class), null, outerJoin).isPresent());

        LogicalJoin conditionalJoin = mock(LogicalJoin.class);
        when(conditionalJoin.getJoinType()).thenReturn(JoinRelType.INNER);
        when(conditionalJoin.getCondition()).thenReturn(rexBuilder.makeLiteral(false));
        assertFalse(VectorJoinPlanExtractor.extract(
                mock(LogicalSort.class), mock(LogicalProject.class), null, conditionalJoin).isPresent());
    }

    @Test
    public void rejectsWrappedOrNonVectorRightInput() {
        LogicalJoin wrapped = mock(LogicalJoin.class);
        when(wrapped.getJoinType()).thenReturn(JoinRelType.INNER);
        when(wrapped.getCondition()).thenReturn(rexBuilder.makeLiteral(true));
        when(wrapped.getRight()).thenReturn(mock(LogicalFilter.class));
        assertFalse(VectorJoinPlanExtractor.extract(
                mock(LogicalSort.class), mock(LogicalProject.class), null, wrapped).isPresent());

        PlanFixture fixture = planFixture(false);
        assertFalse(fixture.extract().isPresent());
    }

    @Test
    public void rejectsOffsetMultipleSortFieldsAndSameSideIp() {
        PlanFixture offset = planFixture(true);
        offset.offset = integer(1);
        assertFalse(offset.extract().isPresent());

        PlanFixture multipleSortFields = planFixture(true);
        multipleSortFields.collation = RelCollations.of(Arrays.asList(
                new RelFieldCollation(0), new RelFieldCollation(1)));
        assertFalse(multipleSortFields.extract().isPresent());

        PlanFixture sameSide = planFixture(true);
        sameSide.setScoreCall(1, 0);
        assertFalse(sameSide.extract().isPresent());
    }

    @Test
    public void acceptsReversedIpOperandsAndPreservesDescendingCollation() {
        PlanFixture fixture = planFixture(true);
        fixture.setScoreCall(3, 1);
        fixture.collation = RelCollations.of(
                new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));

        Optional<VectorJoinPlanExtractor.VectorLookupPlan> extracted = fixture.extract();

        assertTrue(extracted.isPresent());
        assertEquals(1, extracted.get().getLeftEmbeddingIndex());
        assertEquals("embedding", extracted.get().getRightEmbeddingField());
        assertEquals(3, extracted.get().getTopKPerLeftRow());
        assertEquals(
                RelFieldCollation.Direction.DESCENDING,
                extracted.get().getCollation().getFieldCollations().get(0).getDirection());
        assertEquals(4, ((org.apache.calcite.rex.RexInputRef)
                extracted.get().rewriteProjects(4).get(0)).getIndex());
    }

    private VectorJoinPlanExtractor.FilterSplit split(RexNode condition) {
        return VectorJoinPlanExtractor.splitFilter(condition, 2, 2, rexBuilder);
    }

    private RexNode input(int index) {
        return rexBuilder.makeInputRef(
                joinedRowType.getFieldList().get(index).getType(), index);
    }

    private RexNode integer(int value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
    }

    private PlanFixture planFixture(boolean vectorRight) {
        return new PlanFixture(vectorRight);
    }

    private final class PlanFixture {
        private final LogicalProject project = mock(LogicalProject.class);
        private final LogicalJoin join = mock(LogicalJoin.class);
        private final LogicalTableScan rightScan = mock(LogicalTableScan.class);
        private final RelNode left = mock(RelNode.class);
        private final RelOptTable rightTable = mock(RelOptTable.class);
        private final RelDataType leftType;
        private final RelDataType rightType;
        private final RelDataType scoreType;
        private org.apache.calcite.rel.RelCollation collation =
                RelCollations.of(new RelFieldCollation(0));
        private RexNode offset;
        private RexNode fetch = integer(3);

        private PlanFixture(boolean vectorRight) {
            JavaTypeFactoryImpl typeFactory = (JavaTypeFactoryImpl) rexBuilder.getTypeFactory();
            RelOptCluster cluster = RelOptCluster.create(
                    new VolcanoPlanner(), rexBuilder);
            leftType = typeFactory.builder()
                    .add("user_id", SqlTypeName.INTEGER)
                    .add("embedding", SqlTypeName.ANY)
                    .build();
            rightType = typeFactory.builder()
                    .add("item_id", SqlTypeName.INTEGER)
                    .add("embedding", SqlTypeName.ANY)
                    .build();
            scoreType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

            when(join.getJoinType()).thenReturn(JoinRelType.INNER);
            when(join.getCondition()).thenReturn(rexBuilder.makeLiteral(true));
            when(join.getLeft()).thenReturn(left);
            when(join.getRight()).thenReturn(rightScan);
            when(join.getCluster()).thenReturn(cluster);
            when(left.getRowType()).thenReturn(leftType);
            when(rightScan.getRowType()).thenReturn(rightType);
            when(rightScan.getTable()).thenReturn(rightTable);
            when(rightTable.unwrap(VectorSearchable.class)).thenReturn(
                    vectorRight ? mock(VectorSearchable.class) : null);

            when(project.getCluster()).thenReturn(cluster);
            when(project.getTraitSet()).thenReturn(cluster.traitSet());
            when(project.getRowType()).thenReturn(typeFactory.builder()
                    .add("score", scoreType)
                    .build());
            setScoreCall(1, 3);
        }

        private void setScoreCall(int firstIndex, int secondIndex) {
            SqlOperator ip = new SqlFunction(
                    "ip",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE,
                    null,
                    OperandTypes.ANY_ANY,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
            RexCall score = (RexCall) rexBuilder.makeCall(
                    ip,
                    rexBuilder.makeInputRef(
                            firstIndex < 2
                                    ? leftType.getFieldList().get(firstIndex).getType()
                                    : rightType.getFieldList().get(firstIndex - 2).getType(),
                            firstIndex),
                    rexBuilder.makeInputRef(
                            secondIndex < 2
                                    ? leftType.getFieldList().get(secondIndex).getType()
                                    : rightType.getFieldList().get(secondIndex - 2).getType(),
                            secondIndex));
            when(project.getProjects()).thenReturn(Collections.singletonList(score));
        }

        private Optional<VectorJoinPlanExtractor.VectorLookupPlan> extract() {
            LogicalSort sort = LogicalSort.create(
                    project, collation, offset, fetch);
            return VectorJoinPlanExtractor.extract(sort, project, null, join);
        }
    }
}
