package com.sqlrec.rules;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.node.SqlRecJoin;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Map;

public class SqlRecJoinRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    BindableConvention.INSTANCE, "SqlRecJoinRule")
            .withRuleFactory(SqlRecJoinRule::new);

    /**
     * Called from the Config.
     */
    protected SqlRecJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalJoin join = (LogicalJoin) rel;
        boolean isKvTable = isJoinKvTable(join);

        final BindableConvention out = BindableConvention.INSTANCE;
        final RelTraitSet traitSet = join.getTraitSet().replace(out);

        return new SqlRecJoin(rel.getCluster(), traitSet,
                convert(join.getLeft(),
                        join.getLeft().getTraitSet()
                                .replace(BindableConvention.INSTANCE)),
                convert(join.getRight(),
                        join.getRight().getTraitSet()
                                .replace(BindableConvention.INSTANCE)),
                join.getCondition(), join.getVariablesSet(), join.getJoinType());
    }

    private boolean isJoinKvTable(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        SqlRecTable rightKvTable = KvTableUtils.getRightTableKVTable(right);
        if (rightKvTable == null || !(rightKvTable instanceof SqlRecKvTable)) {
            return false;
        }

        if (join.getJoinType() != JoinRelType.LEFT && join.getJoinType() != JoinRelType.INNER) {
            throw new IllegalArgumentException("SqlRecJoinRule only support left join and inner join");
        }

        SqlRecKvTable kvTable = (SqlRecKvTable) rightKvTable;
        RexNode condition = join.getCondition();
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                Map.Entry<Integer, Integer> joinKeyColIndex = KvTableUtils.getJoinKeyColIndex(join);
                int rightJoinFieldIndex = joinKeyColIndex.getValue() - left.getRowType().getFieldCount();
                if (kvTable.getPrimaryKeyIndex() != rightJoinFieldIndex) {
                    throw new IllegalArgumentException("join key column must be equal to right kv table primary key column");
                }
            } else {
                throw new IllegalArgumentException("Join condition must be equal operator");
            }
        } else {
            throw new IllegalArgumentException("Join condition must be equal operator");
        }

        return true;
    }
}
