package com.sqlrec.node;

import com.sqlrec.common.schema.VectorSearchable;
import com.sqlrec.utils.VectorJoinExecutor;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * A two-input lookup join. The left input is enumerated, while the right table is
 * queried through {@link VectorSearchable} for every eligible left row.
 */
public final class SqlrecEnumerableVectorLookupJoin extends BiRel implements EnumerableRel {
    private final RexNode pushedFilter;
    private final int leftEmbeddingIndex;
    private final String rightEmbeddingField;
    private final int topKPerLeftRow;
    private final RelDataType lookupRowType;

    private SqlrecEnumerableVectorLookupJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode pushedFilter,
            int leftEmbeddingIndex,
            String rightEmbeddingField,
            int topKPerLeftRow,
            RelDataType lookupRowType) {
        super(cluster, traitSet, left, right);
        this.pushedFilter = pushedFilter;
        this.leftEmbeddingIndex = leftEmbeddingIndex;
        this.rightEmbeddingField = rightEmbeddingField;
        this.topKPerLeftRow = topKPerLeftRow;
        this.lookupRowType = lookupRowType;
    }

    public static SqlrecEnumerableVectorLookupJoin create(
            RelNode left,
            RelNode right,
            RexNode pushedFilter,
            int leftEmbeddingIndex,
            String rightEmbeddingField,
            int topKPerLeftRow,
            RelDataType scoreType) {
        RelDataType rowType = left.getCluster().getTypeFactory().builder()
                .addAll(left.getRowType().getFieldList())
                .addAll(right.getRowType().getFieldList())
                .add("$vector_score", scoreType)
                .build();
        RelTraitSet traitSet = left.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replace(RelCollations.EMPTY);
        return new SqlrecEnumerableVectorLookupJoin(
                left.getCluster(),
                traitSet,
                left,
                right,
                pushedFilter,
                leftEmbeddingIndex,
                rightEmbeddingField,
                topKPerLeftRow,
                rowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return lookupRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (inputs.size() != 2) {
            throw new IllegalArgumentException("Vector lookup join requires exactly two inputs");
        }
        return new SqlrecEnumerableVectorLookupJoin(
                getCluster(),
                traitSet,
                inputs.get(0),
                inputs.get(1),
                pushedFilter,
                leftEmbeddingIndex,
                rightEmbeddingField,
                topKPerLeftRow,
                lookupRowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter writer) {
        return super.explainTerms(writer)
                .itemIf("pushedFilter", pushedFilter, pushedFilter != null)
                .item("leftEmbeddingIndex", leftEmbeddingIndex)
                .item("rightEmbeddingField", rightEmbeddingField)
                .itemIf("topKPerLeftRow", topKPerLeftRow, topKPerLeftRow > 0);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // The zero cost is intentional: the planner cannot estimate remote ANN
        // lookup cost, and SQLRec must prefer this specialized implementation.
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        if (!(right instanceof TableScan)) {
            throw new IllegalStateException("Right input must be a direct table scan");
        }
        RelOptTable rightTable = ((TableScan) right).getTable();
        VectorSearchable vectorTable = rightTable.unwrap(VectorSearchable.class);
        if (vectorTable == null) {
            throw new IllegalStateException("Right table must implement VectorSearchable");
        }

        BlockBuilder builder = new BlockBuilder();
        Result leftResult = implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression = builder.append("left", leftResult.block);
        Expression rightExpression = implementor.stash(vectorTable, VectorSearchable.class);
        Expression filterExpression = pushedFilter == null
                ? Expressions.constant(null)
                : implementor.stash(pushedFilter, RexNode.class);
        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(), getRowType(), pref.preferArray());

        // The right input remains visible in the relational plan, but it is not
        // enumerated. It is the lookup source used by VectorSearchable.
        Expression lookup = Expressions.call(
                VectorJoinExecutor.class,
                "execute",
                leftExpression,
                rightExpression,
                filterExpression,
                Expressions.constant(leftEmbeddingIndex),
                Expressions.constant(rightEmbeddingField),
                Expressions.constant(topKPerLeftRow));
        builder.add(Expressions.return_(null, lookup));
        return implementor.result(physType, builder.toBlock());
    }

    public RexNode getPushedFilter() {
        return pushedFilter;
    }

    public int getLeftEmbeddingIndex() {
        return leftEmbeddingIndex;
    }

    public String getRightEmbeddingField() {
        return rightEmbeddingField;
    }

    public int getTopKPerLeftRow() {
        return topKPerLeftRow;
    }
}
