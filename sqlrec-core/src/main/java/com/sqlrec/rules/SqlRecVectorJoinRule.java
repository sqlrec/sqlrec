package com.sqlrec.rules;

import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.node.SqlrecEnumerableVectorJoin;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.VectorJoinUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class SqlRecVectorJoinRule extends RelRule<SqlRecVectorJoinRule.Config> {

    protected SqlRecVectorJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        LogicalProject project;
        LogicalFilter filter = null;
        LogicalJoin join;

        if (config.hasFilter()) {
            project = call.rel(1);
            filter = call.rel(2);
            join = call.rel(3);
        } else {
            project = call.rel(1);
            join = call.rel(2);
        }

        if (!NodeUtils.hasIpFunction(project)) {
            return;
        }
        if (!NodeUtils.isTrueCondition(join)) {
            return;
        }
        RelOptTable rightTable = NodeUtils.getScanTable(join.getRight());
        if (rightTable == null || rightTable.unwrap(SqlRecVectorTable.class) == null) {
            return;
        }

        VectorJoinUtils.VectorJoinConfig joinConfig = VectorJoinUtils.extractVectorJoinConfig(
                sort, project, filter, join
        );

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }
        final RelNode left = newInputs.get(0);
        final RelNode right = newInputs.get(1);

        SqlrecEnumerableVectorJoin newJoin = SqlrecEnumerableVectorJoin.create(
                left,
                right,
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                joinConfig.filterCondition,
                joinConfig.leftEmbeddingColIndex,
                joinConfig.rightEmbeddingColName,
                joinConfig.limit,
                joinConfig.projectColumns,
                joinConfig.projectRowType,
                joinConfig.collation
        );

        call.transformTo(newJoin);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        SqlRecVectorJoinRule.Config WITH_FILTER = ImmutableSqlRecVectorJoinRule.Config.builder()
                .hasFilter(true)
                .build()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalSort.class)
                                .inputs(b1 ->
                                        b1.operand(LogicalProject.class)
                                                .inputs(b2 ->
                                                        b2.operand(LogicalFilter.class)
                                                                .inputs(b3 ->
                                                                        b3.operand(LogicalJoin.class)
                                                                                .anyInputs()))))
                .withDescription("SqlRecVectorJoinRule.WithFilter");

        SqlRecVectorJoinRule.Config NO_FILTER = ImmutableSqlRecVectorJoinRule.Config.builder()
                .hasFilter(false)
                .build()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalSort.class)
                                .inputs(b1 ->
                                        b1.operand(LogicalProject.class)
                                                .inputs(b2 ->
                                                        b2.operand(LogicalJoin.class)
                                                                .anyInputs())))
                .withDescription("SqlRecVectorJoinRule.NoFilter");

        @Value.Default
        default boolean hasFilter() {
            return true;
        }

        @Override
        default SqlRecVectorJoinRule toRule() {
            return new SqlRecVectorJoinRule(this);
        }
    }
}
