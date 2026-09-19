package com.sqlrec.node;

import com.sqlrec.common.utils.MergeUtils;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class SqlrecEnumerableUnion extends EnumerableUnion {
    private static final Method MERGE_METHOD = getMergeMethod(
            "snakeMergeEnumerable", Iterable[].class);
    private static final Method DISTINCT_MERGE_METHOD = getMergeMethod(
            "snakeMergeDistinctEnumerable", EqualityComparer.class, Iterable[].class);

    public SqlrecEnumerableUnion(RelOptCluster cluster, RelTraitSet traitSet,
                                 List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public SqlrecEnumerableUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new SqlrecEnumerableUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        List<Expression> inputExps = new ArrayList<>();
        Expression comparer = null;
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild(this, ord.i, input, pref);
            Expression childExp =
                    builder.append(
                            "child" + ord.i,
                            result.block);
            inputExps.add(childExp);
            if (comparer == null) {
                comparer = result.physType.comparer();
            }
        }

        Expression unionExp;
        if (all) {
            unionExp = Expressions.call(MERGE_METHOD, inputExps);
        } else {
            List<Expression> arguments = new ArrayList<>();
            arguments.add(comparer == null
                    ? Expressions.constant(null, EqualityComparer.class)
                    : comparer);
            arguments.addAll(inputExps);
            unionExp = Expressions.call(DISTINCT_MERGE_METHOD, arguments);
        }

        builder.add(unionExp);
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer(JavaRowFormat.CUSTOM));
        return implementor.result(physType, builder.toBlock());
    }

    private static Method getMergeMethod(String name, Class<?>... parameterTypes) {
        try {
            return MergeUtils.class.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
