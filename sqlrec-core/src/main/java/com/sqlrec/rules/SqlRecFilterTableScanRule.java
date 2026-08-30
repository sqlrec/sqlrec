package com.sqlrec.rules;

import com.google.common.collect.ImmutableList;
import com.sqlrec.node.FilterableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

@Value.Enclosing
public class SqlRecFilterTableScanRule extends RelRule<SqlRecFilterTableScanRule.Config> {
    protected SqlRecFilterTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 2) {
            // the ordinary variant
            final Filter filter = call.rel(0);
            final TableScan scan = call.rel(1);
            apply(call, filter, scan);
        } else if (call.rels.length == 3) {
            // the variant with intervening EnumerableInterpreter
            final Filter filter = call.rel(0);
            final TableScan scan = call.rel(2);
            apply(call, filter, scan);
        } else {
            throw new AssertionError();
        }
    }

    public static boolean test(TableScan scan) {
        // The custom scan implementation only supports FilterableTable. Do not
        // match ProjectableFilterableTable until projection pushdown is implemented.
        final RelOptTable table = scan.getTable();
        return table.unwrap(FilterableTable.class) != null;
    }

    protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
        final ImmutableIntList projects = scan.identity();
        final ImmutableList.Builder<RexNode> filters = ImmutableList.builder();
        final Mapping mapping = Mappings.target(projects,
                scan.getTable().getRowType().getFieldCount());
        filters.add(
                RexUtil.apply(mapping.inverse(), filter.getCondition()));
        TableScan kvTableScan = FilterableTableScan.create(
                scan.getCluster(), scan.getTable(), filters.build(), projects
        );
        // FilterableTableScan is an Enumerable-native custom node and does not perform
        // Calcite's runtime "remaining mutable filters" negotiation. Keep the complete
        // original predicate as a residual condition for every connector.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RelDataType inputRowType = kvTableScan.getRowType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(filter.getCondition());
        final RexProgram program = programBuilder.getProgram();
        call.transformTo(LogicalCalc.create(kvTableScan, program));
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        SqlRecFilterTableScanRule.Config DEFAULT = ImmutableSqlRecFilterTableScanRule.Config.builder().build()
                .withOperandSupplier(b0 ->
                        b0.operand(Filter.class).oneInput(b1 ->
                                b1.operand(TableScan.class)
                                        .predicate(SqlRecFilterTableScanRule::test).noInputs()));

        SqlRecFilterTableScanRule.Config INTERPRETER = ImmutableSqlRecFilterTableScanRule.Config.builder().build()
                .withOperandSupplier(b0 ->
                        b0.operand(Filter.class).oneInput(b1 ->
                                b1.operand(EnumerableInterpreter.class).oneInput(b2 ->
                                        b2.operand(TableScan.class)
                                                .predicate(SqlRecFilterTableScanRule::test).noInputs())))
                .withDescription("SqlRecFilterTableScanRule:interpreter");

        @Override
        default SqlRecFilterTableScanRule toRule() {
            return new SqlRecFilterTableScanRule(this);
        }
    }
}
