package com.sqlrec.node;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.utils.KvJoinUtils;
import com.sqlrec.utils.NodeUtils;
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

public class SqlrecEnumerableKvJoin extends EnumerableNestedLoopJoin {

    protected SqlrecEnumerableKvJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType
    ) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
    }

    public static SqlrecEnumerableKvJoin create(
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType
    ) {
        return new SqlrecEnumerableKvJoin(
                left.getCluster(),
                left.getTraitSet(),
                left,
                right,
                condition,
                variablesSet,
                joinType
        );
    }

    @Override
    public SqlrecEnumerableKvJoin copy(RelTraitSet traitSet,
                                       RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                                       boolean semiJoinDone) {
        return new SqlrecEnumerableKvJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        RelOptTable rightTable = NodeUtils.getScanTable(right);
        if (rightTable == null || rightTable.unwrap(SqlRecKvTable.class) == null) {
            return super.implement(implementor, pref);
        }

        RelOptSchema relOptSchema = rightTable.getRelOptSchema();
        if (!(relOptSchema instanceof CalciteCatalogReader)) {
            throw new IllegalArgumentException("SqlrecEnumerableKvJoin only support CalciteCatalogReader");
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
        rightExpression = Expressions.convert_(rightExpression, SqlRecKvTable.class);

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
                        Expressions.call(
                                KvJoinUtils.class,
                                "kvJoin",
                                leftExpression,
                                rightExpression,
                                stashedCondition,
                                Expressions.constant(joinType)
                        )
                ).toBlock()
        );
    }
}
