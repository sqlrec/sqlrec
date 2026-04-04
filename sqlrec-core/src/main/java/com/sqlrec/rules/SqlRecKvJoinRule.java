package com.sqlrec.rules;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.node.SqlrecEnumerableKvJoin;
import com.sqlrec.utils.KvJoinUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class SqlRecKvJoinRule extends RelRule<SqlRecKvJoinRule.Config> {

    protected SqlRecKvJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RexNode condition = join.getCondition();
        try {
            KvJoinUtils.getJoinKeyColIndex(condition);
        } catch (Exception e) {
            return;
        }

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }
        final RelNode left = newInputs.get(0);
        final RelNode right = newInputs.get(1);

        SqlrecEnumerableKvJoin newJoin = SqlrecEnumerableKvJoin.create(
                left,
                right,
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType()
        );

        call.transformTo(newJoin);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        SqlRecKvJoinRule.Config DEFAULT = ImmutableSqlRecKvJoinRule.Config.builder()
                .build()
                .withOperandSupplier(b1 ->
                        b1.operand(LogicalJoin.class).anyInputs())
                .withDescription("SqlRecKvJoinRule");

        @Override
        default SqlRecKvJoinRule toRule() {
            return new SqlRecKvJoinRule(this);
        }
    }
}
