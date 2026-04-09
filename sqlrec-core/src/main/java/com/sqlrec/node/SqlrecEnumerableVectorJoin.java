package com.sqlrec.node;

import com.google.common.collect.ImmutableList;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.VectorJoinUtils;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;
import java.util.Set;

public class SqlrecEnumerableVectorJoin extends AbstractRelNode implements EnumerableRel {
    private RelNode left;
    private RelNode right;
    private RexNode condition;
    private Set<CorrelationId> variablesSet;
    private JoinRelType joinType;
    private RexNode filterCondition;
    private int leftEmbeddingColIndex;
    private String rightEmbeddingColName;
    private int limit;
    private List<Integer> projectList;

    protected SqlrecEnumerableVectorJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexNode filterCondition,
            int leftEmbeddingColIndex,
            String rightEmbeddingColName,
            int limit,
            List<Integer> projectList,
            RelDataType projectRowType
    ) {
        super(cluster, traits);
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.variablesSet = variablesSet;
        this.joinType = joinType;
        this.filterCondition = filterCondition;
        this.leftEmbeddingColIndex = leftEmbeddingColIndex;
        this.rightEmbeddingColName = rightEmbeddingColName;
        this.limit = limit;
        this.projectList = projectList;
        try {
            java.lang.reflect.Field rowTypeField = AbstractRelNode.class.getDeclaredField("rowType");
            rowTypeField.setAccessible(true);
            rowTypeField.set(this, projectRowType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set rowType", e);
        }
    }

    public static SqlrecEnumerableVectorJoin create(
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexNode filterCondition,
            int leftEmbeddingColIndex,
            String rightEmbeddingColName,
            int limit,
            List<Integer> projectList,
            RelDataType projectRowType,
            RelCollation collation
    ) {
        RelTraitSet traitSet = left.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replaceIf(RelCollationTraitDef.INSTANCE, () -> collation);
        
        return new SqlrecEnumerableVectorJoin(
                left.getCluster(),
                traitSet,
                left,
                right,
                condition,
                variablesSet,
                joinType,
                filterCondition,
                leftEmbeddingColIndex,
                rightEmbeddingColName,
                limit,
                projectList,
                projectRowType
        );
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SqlrecEnumerableVectorJoin(
                getCluster(),
                traitSet,
                inputs.get(0),
                inputs.get(1),
                condition,
                variablesSet,
                joinType,
                filterCondition,
                leftEmbeddingColIndex,
                rightEmbeddingColName,
                limit,
                projectList,
                getRowType()
        );
    }

    @Override
    public List<RelNode> getInputs() {
        return ImmutableList.of(left, right);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        if (ordinalInParent == 0) {
            this.left = p;
        } else if (ordinalInParent == 1) {
            this.right = p;
        } else {
            throw new IllegalArgumentException("ordinalInParent must be 0 or 1, got " + ordinalInParent);
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("left", left)
                .item("right", right)
                .item("condition", condition)
                .item("joinType", joinType)
                .itemIf("filterCondition", filterCondition, filterCondition != null)
                .item("leftEmbeddingColIndex", leftEmbeddingColIndex)
                .item("rightEmbeddingColName", rightEmbeddingColName)
                .itemIf("limit", limit, limit > 0)
                .itemIf("projects", projectList, projectList != null && !projectList.isEmpty());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        RelOptTable rightTable = NodeUtils.getScanTable(right);
        if (rightTable == null || rightTable.unwrap(SqlRecVectorTable.class) == null) {
            throw new IllegalArgumentException("Right table must be SqlRecVectorTable");
        }

        RelOptSchema relOptSchema = rightTable.getRelOptSchema();
        if (!(relOptSchema instanceof CalciteCatalogReader)) {
            throw new IllegalArgumentException("SqlrecEnumerableVectorJoin only support CalciteCatalogReader");
        }

        CalciteCatalogReader catalogReader = (CalciteCatalogReader) relOptSchema;
        List<String> qualifiedName = rightTable.getQualifiedName();
        CalciteSchema.TableEntry tableEntry = SqlValidatorUtil.getTableEntry(catalogReader, qualifiedName);
        if (tableEntry == null) {
            throw new IllegalArgumentException("Table not found: " + qualifiedName);
        }

        Expression rightExpression = Expressions.call(
                Schemas.expression(tableEntry.schema.plus()),
                BuiltInMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(tableEntry.name));
        rightExpression = Expressions.convert_(rightExpression, SqlRecVectorTable.class);

        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression =
                builder.append("left", leftResult.block);

        final Expression stashedFilterCondition = filterCondition != null ?
                implementor.stash(filterCondition, RexNode.class) :
                Expressions.constant(null);

        final PhysType physType =
                PhysTypeImpl.of(implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferArray());

        return implementor.result(
                physType,
                builder.append(
                                Expressions.call(VectorJoinUtils.class,
                                        "vectorJoin",
                                        leftExpression,
                                        rightExpression,
                                        stashedFilterCondition,
                                        Expressions.constant(leftEmbeddingColIndex),
                                        Expressions.constant(rightEmbeddingColName),
                                        Expressions.constant(limit),
                                        Expressions.constant(projectList)))
                        .toBlock());
    }

    public RelNode getLeft() {
        return left;
    }

    public RelNode getRight() {
        return right;
    }

    public RexNode getCondition() {
        return condition;
    }

    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    public RexNode getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(RexNode filterCondition) {
        this.filterCondition = filterCondition;
    }

    public int getLeftEmbeddingColIndex() {
        return leftEmbeddingColIndex;
    }

    public void setLeftEmbeddingColIndex(int leftEmbeddingColIndex) {
        this.leftEmbeddingColIndex = leftEmbeddingColIndex;
    }

    public String getRightEmbeddingColName() {
        return rightEmbeddingColName;
    }

    public void setRightEmbeddingColName(String rightEmbeddingColName) {
        this.rightEmbeddingColName = rightEmbeddingColName;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public List<Integer> getProjectList() {
        return projectList;
    }

    public void setProjectList(List<Integer> projectList) {
        this.projectList = projectList;
    }
}
