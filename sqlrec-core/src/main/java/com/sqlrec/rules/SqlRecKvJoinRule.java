package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableKvJoin;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class SqlRecKvJoinRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "SqlRecKvJoinRule")
            .withRuleFactory(SqlRecKvJoinRule::new);

    protected SqlRecKvJoinRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        try {
            NodeUtils.getJoinKeyColIndex(join.getCondition());
        } catch (Exception e) {
            return null;
        }

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }

        return SqlrecEnumerableKvJoin.create(
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType()
        );
    }
}
