package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableUnion;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;


@Value.Enclosing
public class SqlRecUnionRule extends RelRule<SqlRecUnionRule.Config> {

    protected SqlRecUnionRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : union.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }

        SqlrecEnumerableUnion newUnion = new SqlrecEnumerableUnion(
                union.getCluster(),
                union.getTraitSet().replace(EnumerableConvention.INSTANCE),
                newInputs,
                union.all
        );

        call.transformTo(newUnion);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        SqlRecUnionRule.Config DEFAULT = ImmutableSqlRecUnionRule.Config.builder()
                .build()
                .withOperandSupplier(b1 ->
                        b1.operand(LogicalUnion.class).anyInputs())
                .withDescription("SqlRecUnionRule");

        @Override
        default SqlRecUnionRule toRule() {
            return new SqlRecUnionRule(this);
        }
    }
}
