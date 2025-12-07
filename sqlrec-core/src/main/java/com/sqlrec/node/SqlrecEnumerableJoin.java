package com.sqlrec.node;

import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.utils.JoinUtils;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;
import java.util.Set;

public class SqlrecEnumerableJoin extends EnumerableNestedLoopJoin {
    private int limit;
    private List<Integer> projectList;

    protected SqlrecEnumerableJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            int limit,
            List<Integer> projectList
    ) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
        this.limit = limit;
        this.projectList = projectList;
    }

    public static SqlrecEnumerableJoin create(
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            int limit,
            List<Integer> projectList
    ) {
        return new SqlrecEnumerableJoin(
                left.getCluster(),
                left.getTraitSet(),
                left,
                right,
                condition,
                variablesSet,
                joinType,
                limit,
                projectList
        );
    }

    @Override
    public SqlrecEnumerableJoin copy(RelTraitSet traitSet,
                                     RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                                     boolean semiJoinDone) {
        return new SqlrecEnumerableJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType, limit, projectList);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("projects", projectList, !projectList.isEmpty())
                .itemIf("limit", limit, limit > 0);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        RelOptTable rightTable = KvTableUtils.getScanTable(right);
        if (rightTable == null || rightTable.unwrap(SqlRecTable.class) == null) {
            return super.implement(implementor, pref);
        }

        RelOptSchema relOptSchema = rightTable.getRelOptSchema();
        if (!(relOptSchema instanceof CalciteCatalogReader)) {
            throw new IllegalArgumentException("FilterableTableScan only support CalciteCatalogReader");
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
        rightExpression = Expressions.convert_(rightExpression, SqlRecTable.class);

        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression =
                builder.append("left", leftResult.block);


        final Expression stashedCondition = implementor.stash(condition, RexNode.class);

        final PhysType physType =
                PhysTypeImpl.of(implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferArray());


        return implementor.result(
                physType,
                builder.append(
                                Expressions.call(JoinUtils.class,
                                        "join",
                                        leftExpression,
                                        rightExpression,
                                        stashedCondition,
                                        Expressions.constant(joinType),
                                        Expressions.constant(limit),
                                        Expressions.constant(projectList)))
                        .toBlock());
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
