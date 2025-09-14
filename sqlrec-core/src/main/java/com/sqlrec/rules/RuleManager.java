package com.sqlrec.rules;

import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.config.CalciteSystemProperty;
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

    public static VolcanoPlanner createPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        RelOptUtil.registerDefaultRules(planner, false, true);

        addSqlRecRules(planner);
        return planner;
    }

    private static void addSqlRecRules(VolcanoPlanner planner) {
        planner.removeRule(CoreRules.FILTER_SCAN);
        planner.addRule(FILTER_SCAN);

        planner.removeRule(CoreRules.FILTER_INTERPRETER_SCAN);
        planner.addRule(FILTER_INTERPRETER_SCAN);

        HmsSchema.getTableFactorieMap()
                .values()
                .forEach(tableFactory -> tableFactory.getRules().forEach(planner::addRule));
    }
}
