package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableVectorLookupJoin;
import com.sqlrec.utils.VectorJoinPlanExtractor;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

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

        Optional<VectorJoinPlanExtractor.VectorLookupPlan> extracted =
                VectorJoinPlanExtractor.extract(sort, project, filter, join);
        if (!extracted.isPresent()) {
            return;
        }
        VectorJoinPlanExtractor.VectorLookupPlan plan = extracted.get();

        RelNode logicalLeft = join.getLeft();
        if (plan.getLeftFilter() != null) {
            logicalLeft = LogicalFilter.create(logicalLeft, plan.getLeftFilter());
        }
        RelNode left = convert(
                logicalLeft,
                logicalLeft.getTraitSet().replace(EnumerableConvention.INSTANCE));
        RelNode logicalRight = plan.getRightScan();
        RelNode right = convert(
                logicalRight,
                logicalRight.getTraitSet().replace(EnumerableConvention.INSTANCE));

        SqlrecEnumerableVectorLookupJoin lookupJoin =
                SqlrecEnumerableVectorLookupJoin.create(
                        left,
                        right,
                        plan.getPushedFilter(),
                        plan.getLeftEmbeddingIndex(),
                        plan.getRightEmbeddingField(),
                        plan.getTopKPerLeftRow(),
                        plan.getScoreType());

        List<RexNode> rewrittenProjects =
                plan.rewriteProjects(lookupJoin.getRowType().getFieldCount() - 1);
        EnumerableProject enumerableProject = EnumerableProject.create(
                lookupJoin, rewrittenProjects, plan.getProjectRowType());

        // ORDER BY ... LIMIT is SQLRec's vector lookup syntax. The connector defines
        // ANN ranking and returns top-K rows per left row, so consuming the Sort is
        // intentional. Preserve the requested collation trait to prevent Calcite from
        // adding a global Sort/Limit.
        RelNode result = enumerableProject.copy(
                enumerableProject.getTraitSet().replace(plan.getCollation()),
                lookupJoin,
                rewrittenProjects,
                plan.getProjectRowType());
        call.transformTo(result);
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
