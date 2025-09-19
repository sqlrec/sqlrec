package org.apache.calcite.interpreter;

import com.google.common.collect.ImmutableList;
import com.sqlrec.node.KvJoinNode;
import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;

public class Nodes {
    public static class CoreCompiler extends Interpreter.CompilerImpl {
        CoreCompiler(@UnknownInitialization Interpreter interpreter, RelOptCluster cluster) {
            super(interpreter, cluster);
        }

        public void visit(Aggregate agg) {
            node = new AggregateNode(this, agg);
        }

        public void visit(Filter filter) {
            node = new FilterNode(this, filter);
        }

        public void visit(Project project) {
            node = new ProjectNode(this, project);
        }

        public void visit(Values value) {
            node = new ValuesNode(this, value);
        }

        public void visit(TableScan scan) {
            final ImmutableList<RexNode> filters = ImmutableList.of();
            node = TableScanNode.create(this, scan, filters, null);
        }

        public void visit(Bindables.BindableTableScan scan) {
            node = TableScanNode.create(this, scan, scan.filters, scan.projects);
        }

        public void visit(TableFunctionScan functionScan) {
            node = TableFunctionScanNode.create(this, functionScan);
        }

        public void visit(Sort sort) {
            node = new SortNode(this, sort);
        }

        public void visit(SetOp setOp) {
            node = new SetOpNode(this, setOp);
        }

        public void visit(Join join) {
            if (KvTableUtils.isKvTable(join.getRight().getTable())) {
                node = new KvJoinNode(this, join);
            } else {
                node = new JoinNode(this, join);
            }
        }

        public void visit(Window window) {
            node = new WindowNode(this, window);
        }

        public void visit(Match match) {
            node = new MatchNode(this, match);
        }

        public void visit(Collect collect) {
            node = new CollectNode(this, collect);
        }

        public void visit(Uncollect uncollect) {
            node = new UncollectNode(this, uncollect);
        }
    }
}
