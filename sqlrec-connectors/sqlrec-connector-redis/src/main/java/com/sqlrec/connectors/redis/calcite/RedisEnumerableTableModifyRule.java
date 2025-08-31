package com.sqlrec.connectors.redis.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedisEnumerableTableModifyRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalTableModify.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "RedisEnumerableTableModifyRule")
            .withRuleFactory(RedisEnumerableTableModifyRule::new);

    protected RedisEnumerableTableModifyRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final TableModify modify = (TableModify) rel;
        final ModifiableTable modifiableTable =
                modify.getTable().unwrap(ModifiableTable.class);
        if (modifiableTable == null) {
            return null;
        }
        final RelTraitSet traitSet = modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
        return new RedisEnumerableTableModify(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), traitSet),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelOptTableImpl table = (RelOptTableImpl) call.rel(0).getTable();
        if (table != null && table.unwrap(RedisCalciteTable.class) != null) {
            return true;
        }
        return false;
    }
}
