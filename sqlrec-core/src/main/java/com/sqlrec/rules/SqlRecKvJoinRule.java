package com.sqlrec.rules;

import com.sqlrec.node.SqlrecEnumerableKvJoin;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

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

        return SqlrecEnumerableKvJoin.create(
                convert(join.getLeft(), EnumerableConvention.INSTANCE),
                convert(join.getRight(), EnumerableConvention.INSTANCE),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType()
        );
    }
}
