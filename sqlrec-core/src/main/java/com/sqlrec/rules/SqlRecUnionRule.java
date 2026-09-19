package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableUnion;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class SqlRecUnionRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalUnion.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "SqlRecUnionRule")
            .withRuleFactory(SqlRecUnionRule::new);

    protected SqlRecUnionRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalUnion union = (LogicalUnion) rel;

        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : union.getInputs()) {
            if (!(input.getConvention() instanceof EnumerableConvention)) {
                input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
            }
            newInputs.add(input);
        }

        return new SqlrecEnumerableUnion(
                union.getCluster(),
                union.getTraitSet().replace(EnumerableConvention.INSTANCE),
                newInputs,
                union.all
        );
    }
}
