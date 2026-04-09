package com.sqlrec.rules;

import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.List;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

public class SqlRecFilterIntoJoinRule extends FilterJoinRule.FilterIntoJoinRule {
    protected SqlRecFilterIntoJoinRule(FilterIntoJoinRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        if (NodeUtils.isScanKVTable(right) && !isFilterOnLeftTable(filter, left)) {
            return;
        }

        super.onMatch(call);
    }

    private boolean isFilterOnLeftTable(Filter filter, RelNode left) {
        if (filter == null) {
            return true;
        }

        int leftInputFieldNum = left.getRowType().getFieldCount();
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, leftInputFieldNum);
        List<RexNode> conjunctions = getConjunctions(filter);
        for (RexNode node : conjunctions) {
            final RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(node);
            final ImmutableBitSet inputBits = inputFinder.build();
            if (!leftBitmap.contains(inputBits)) {
                return false;
            }
        }
        return true;
    }

    private static List<RexNode> getConjunctions(Filter filter) {
        List<RexNode> conjunctions = conjunctions(filter.getCondition());
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        for (int i = 0; i < conjunctions.size(); i++) {
            RexNode node = conjunctions.get(i);
            if (node instanceof RexCall) {
                conjunctions.set(i,
                        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
            }
        }
        return conjunctions;
    }

    @Value.Immutable(singleton = false)
    public interface SqlRecFilterIntoJoinRuleConfig extends FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig {
        SqlRecFilterIntoJoinRuleConfig DEFAULT =
                ImmutableSqlRecFilterIntoJoinRuleConfig.of((join, joinType, exp) -> true)
                        .withOperandSupplier(b0 ->
                                b0.operand(Filter.class).oneInput(b1 ->
                                        b1.operand(Join.class).anyInputs()));

        @Value.Default
        default boolean isSmart() {
            return true;
        }

        default Config withSmart(boolean smart) {
            return this;
        }

        @Override
        default SqlRecFilterIntoJoinRule toRule() {
            return new SqlRecFilterIntoJoinRule(this);
        }
    }
}
