package com.sqlrec.rules;

import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;

public class RuleManager {
    public static final SqlRecFilterTableScanRule FILTER_SCAN =
            SqlRecFilterTableScanRule.Config.DEFAULT.toRule();
    public static final SqlRecFilterTableScanRule FILTER_INTERPRETER_SCAN =
            SqlRecFilterTableScanRule.Config.INTERPRETER.toRule();
    public static final SqlRecFilterIntoJoinRule FILTER_INTO_JOIN =
            SqlRecFilterIntoJoinRule.SqlRecFilterIntoJoinRuleConfig.DEFAULT.toRule();
    public static final SqlRecJoinRule SQL_REC_JOIN_RULE =
            SqlRecJoinRule.DEFAULT_CONFIG.toRule(SqlRecJoinRule.class);
    public static final SqlRecLimitToJoinRule LIMIT_TO_JOIN =
            SqlRecLimitToJoinRule.Config.DEFAULT.toRule();

    public static VolcanoPlanner createPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        RelOptUtil.registerDefaultRules(planner, false, true);

        // todo don't modify the default rules when don't access outside storage
        addSqlRecRules(planner);
        return planner;
    }

    private static void addSqlRecRules(VolcanoPlanner planner) {
        planner.removeRule(CoreRules.FILTER_SCAN);
        planner.addRule(FILTER_SCAN);

        planner.removeRule(CoreRules.FILTER_INTERPRETER_SCAN);
        planner.addRule(FILTER_INTERPRETER_SCAN);

        planner.removeRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(FILTER_INTO_JOIN);

        planner.removeRule(Bindables.BINDABLE_JOIN_RULE);
        planner.addRule(LIMIT_TO_JOIN);

        planner.removeRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        planner.removeRule(CoreRules.JOIN_COMMUTE);
        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        HmsSchema.getTableFactorieMap()
                .values()
                .forEach(tableFactory -> tableFactory.getRules().forEach(planner::addRule));
    }
}
