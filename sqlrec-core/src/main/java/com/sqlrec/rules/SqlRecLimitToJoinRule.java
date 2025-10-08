package com.sqlrec.rules;

import com.sqlrec.node.SqlRecJoin;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.immutables.value.Value;

import java.util.ArrayList;

@Value.Enclosing
public class SqlRecLimitToJoinRule extends RelRule<SqlRecLimitToJoinRule.Config> {
    protected SqlRecLimitToJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelOptPlanner planner = call.getPlanner();
        RelNode originalRoot = getOriginalRoot(planner);

        LogicalJoin join = call.rel(0);
        KvTableUtils.JoinPostProcessConfig joinPostProcessConfig = KvTableUtils.findJoinPostProcessConfig(originalRoot, join);

        final BindableConvention out = BindableConvention.INSTANCE;
        final RelTraitSet traitSet = join.getTraitSet().replace(out);

        SqlRecJoin newJoin = new SqlRecJoin(join.getCluster(), traitSet,
                convert(join.getLeft(),
                        join.getLeft().getTraitSet()
                                .replace(BindableConvention.INSTANCE)),
                convert(join.getRight(),
                        join.getRight().getTraitSet()
                                .replace(BindableConvention.INSTANCE)),
                join.getCondition(), join.getVariablesSet(), join.getJoinType());

        if (joinPostProcessConfig != null) {
            newJoin.setLimit(joinPostProcessConfig.limit);
            newJoin.setProjectList(joinPostProcessConfig.projectColumns);
        } else {
            newJoin.setLimit(0);
            newJoin.setProjectList(new ArrayList<>());
        }

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
        SqlRecLimitToJoinRule.Config DEFAULT = ImmutableSqlRecLimitToJoinRule.Config.builder().build()
                .withOperandSupplier(b1 ->
                        b1.operand(LogicalJoin.class).anyInputs())
                .withDescription("SqlRecLimitToJoinRule4");

        @Override
        default SqlRecLimitToJoinRule toRule() {
            return new SqlRecLimitToJoinRule(this);
        }
    }
}
