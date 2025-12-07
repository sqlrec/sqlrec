package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableJoin;
import com.sqlrec.utils.JoinUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class SqlRecJoinRule extends RelRule<SqlRecJoinRule.Config> {
    protected SqlRecJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelOptPlanner planner = call.getPlanner();
        RelNode originalRoot = getOriginalRoot(planner);

        LogicalJoin join = call.rel(0);
        JoinUtils.JoinPostProcessConfig joinPostProcessConfig = JoinUtils.findJoinPostProcessConfig(
                originalRoot, join
        );

        int limit = 0;
        List<Integer> projectList = new ArrayList<>();
        if (joinPostProcessConfig != null) {
            limit = joinPostProcessConfig.limit;
            projectList = joinPostProcessConfig.projectColumns;
        }

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input =
                        convert(
                                input,
                                input.getTraitSet()
                                        .replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }
        final RelNode left = newInputs.get(0);
        final RelNode right = newInputs.get(1);

        SqlrecEnumerableJoin newJoin = SqlrecEnumerableJoin.create(
                left,
                right,
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                limit,
                projectList
        );

        call.transformTo(newJoin);
    }


    RelNode getOriginalRoot(RelOptPlanner planner) {
        if (!(planner instanceof VolcanoPlanner)) {
            throw new IllegalArgumentException("only support VolcanoPlanner");
        }
        VolcanoPlanner volcanoPlanner = (VolcanoPlanner) planner;
        // use reflect to get private field originalRoot;
        try {
            java.lang.reflect.Field field = VolcanoPlanner.class.getDeclaredField("originalRoot");
            field.setAccessible(true);
            return (RelNode) field.get(volcanoPlanner);
        } catch (Exception e) {
            throw new RuntimeException("get originalRoot failed", e);
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        SqlRecJoinRule.Config DEFAULT = ImmutableSqlRecJoinRule.Config.builder().build()
                .withOperandSupplier(b1 ->
                        b1.operand(LogicalJoin.class).anyInputs())
                .withDescription("SqlRecLimitToJoinRule4");

        @Override
        default SqlRecJoinRule toRule() {
            return new SqlRecJoinRule(this);
        }
    }
}
