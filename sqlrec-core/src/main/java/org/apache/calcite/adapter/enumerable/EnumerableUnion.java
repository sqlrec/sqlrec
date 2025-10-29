package org.apache.calcite.adapter.enumerable;

import com.sqlrec.utils.MergeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Union} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableUnion extends Union implements EnumerableRel {
    public EnumerableUnion(RelOptCluster cluster, RelTraitSet traitSet,
                           List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public EnumerableUnion copy(RelTraitSet traitSet, List<RelNode> inputs,
                                boolean all) {
        return new EnumerableUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        List<Expression> inputExps = new ArrayList<>();
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild(this, ord.i, input, pref);
            Expression childExp =
                    builder.append(
                            "child" + ord.i,
                            result.block);
            inputExps.add(childExp);
        }

        Expression unionExp = null;
        try {
            unionExp = Expressions.call(MergeUtils.class.getMethod("snakeMerge", Enumerable[].class), inputExps);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        builder.add(requireNonNull(unionExp, "unionExp"));
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer(JavaRowFormat.CUSTOM));
        return implementor.result(physType, builder.toBlock());
    }
}
