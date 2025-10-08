package com.sqlrec.node;

import com.sqlrec.utils.KvTableUtils;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.JoinNode;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

public class SqlRecJoin extends Bindables.BindableJoin {
    private int limit;
    private List<Integer> projectList;

    public SqlRecJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public SqlRecJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                           RelNode left, RelNode right, JoinRelType joinType,
                           boolean semiJoinDone) {
        SqlRecJoin join = new SqlRecJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
        join.limit = limit;
        join.projectList = projectList;
        return join;
    }

    @Override
    public void childrenAccept(RelVisitor visitor) {
        visitor.visit(left, 0, this);
        if (KvTableUtils.getRightTableKVTable(right) == null) {
            visitor.visit(right, 1, this);
        }
    }

    @Override
    public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
        return super.bind(dataContext);
    }

    @Override
    public Node implement(InterpreterImplementor implementor) {
        return new JoinNode(implementor.compiler, this);
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public List<Integer> getProjectList() {
        return projectList;
    }

    public void setProjectList(List<Integer> projectList) {
        this.projectList = projectList;
    }
}
