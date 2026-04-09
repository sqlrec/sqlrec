package com.sqlrec.rules;

import com.sqlrec.schema.TableFactoryUtils;
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
    public static final SqlRecKvJoinRule KV_JOIN =
            SqlRecKvJoinRule.Config.DEFAULT.toRule();
    public static final SqlRecVectorJoinRule VECTOR_JOIN_WITH_FILTER =
            SqlRecVectorJoinRule.Config.WITH_FILTER.toRule();
    public static final SqlRecVectorJoinRule VECTOR_JOIN_NO_FILTER =
            SqlRecVectorJoinRule.Config.NO_FILTER.toRule();
    public static final SqlRecTableModifyRule SQLREC_TABLE_MODIFY =
            SqlRecTableModifyRule.DEFAULT_CONFIG.toRule(SqlRecTableModifyRule.class);

    public static VolcanoPlanner createPlanner(boolean addKvTableRules) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        RelOptUtil.registerDefaultRules(planner, false, true);

        planner.removeRule(EnumerableRules.TO_INTERPRETER);

        if (addKvTableRules) {
            addKvTableRules(planner);
        }

        planner.removeRule(EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE);
        planner.addRule(SQLREC_TABLE_MODIFY);

        addTableFactoryRules(planner);
        return planner;
    }

    private static void addKvTableRules(VolcanoPlanner planner) {
        planner.removeRule(CoreRules.FILTER_SCAN);
        planner.addRule(FILTER_SCAN);

        planner.removeRule(CoreRules.FILTER_INTERPRETER_SCAN);
        planner.addRule(FILTER_INTERPRETER_SCAN);

        planner.removeRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(FILTER_INTO_JOIN);

        planner.removeRule(Bindables.BINDABLE_JOIN_RULE);
        planner.addRule(KV_JOIN);
        planner.addRule(VECTOR_JOIN_WITH_FILTER);
        planner.addRule(VECTOR_JOIN_NO_FILTER);

        planner.removeRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        planner.removeRule(CoreRules.JOIN_COMMUTE);
        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    }

    private static void addTableFactoryRules(VolcanoPlanner planner) {
        TableFactoryUtils.getTableFactoryMap()
                .values()
                .forEach(tableFactory -> tableFactory.getRules().forEach(planner::addRule));
    }
}
