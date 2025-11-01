package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.connector.CalciteEnumerableTableModify;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class MilvusEnumerableTableModify extends CalciteEnumerableTableModify {
    public MilvusEnumerableTableModify(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode child,
            Operation operation,
            @Nullable List<String> updateColumnList,
            @Nullable List<RexNode> sourceExpressionList,
            boolean flattened) {
        super(cluster,
                traits,
                table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MilvusEnumerableTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened());
    }
}
