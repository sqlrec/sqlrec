package com.sqlrec.node;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// UPDATE is intentionally emitted as an insert-like operation. Downstream
// storage guarantees UPSERT semantics, so rows with the same key replace the
// existing rows instead of creating duplicates.
public class SqlrecEnumerableTableModify extends EnumerableTableModify {
    public SqlrecEnumerableTableModify(
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
                flattened
        );
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SqlrecEnumerableTableModify(
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

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result result =
                implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        Expression childExp =
                builder.append(
                        "child", result.block);
        final ParameterExpression collectionParameter =
                Expressions.parameter(Collection.class,
                        builder.newName("collection"));
        final Expression expression = table.getExpression(ModifiableTable.class);
        if (expression == null) {
            throw new IllegalStateException("ModifiableTable expression is missing");
        }
        if (!ModifiableTable.class.isAssignableFrom(Types.toClass(expression.getType()))) {
            throw new IllegalStateException(
                    "Expected a ModifiableTable expression, got " + expression.getType());
        }
        builder.add(
                Expressions.declare(
                        Modifier.FINAL,
                        collectionParameter,
                        Expressions.call(
                                expression,
                                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION
                                        .method)));
        final Expression countParameter =
                builder.append(
                        "count",
                        Expressions.call(collectionParameter, "size"),
                        false);
        Expression convertedChildExp;
        if (!getInput().getRowType().equals(getRowType())) {
            final JavaTypeFactory typeFactory =
                    (JavaTypeFactory) getCluster().getTypeFactory();
            final JavaRowFormat format = EnumerableTableScan.deduceFormat(table);
            PhysType physType =
                    PhysTypeImpl.of(typeFactory, table.getRowType(), format);
            List<Expression> expressionList = new ArrayList<>();
            final PhysType childPhysType = result.physType;
            final ParameterExpression o_ =
                    Expressions.parameter(childPhysType.getJavaRowType(), "o");
            final int fieldCount =
                    childPhysType.getRowType().getFieldCount();

            // select column here
            if (getOperation().equals(Operation.UPDATE) && getUpdateColumnList() != null) {
                for (int i = 0; i < fieldCount - getUpdateColumnList().size(); i++) {
                    int relIndex = i;
                    String fieldName = getInput().getRowType().getFieldList().get(i).getName();
                    if (getUpdateColumnList().contains(fieldName)) {
                        relIndex = getInput().getRowType().getFieldList().size()
                                - getUpdateColumnList().size() + getUpdateColumnList().indexOf(fieldName);
                    }
                    expressionList.add(
                            childPhysType.fieldReference(o_, relIndex, physType.getJavaFieldType(i)));
                }
            } else {
                for (int i = 0; i < fieldCount; i++) {
                    expressionList.add(
                            childPhysType.fieldReference(o_, i, physType.getJavaFieldType(i)));
                }
            }

            convertedChildExp =
                    builder.append(
                            "convertedChild",
                            Expressions.call(
                                    childExp,
                                    BuiltInMethod.SELECT.method,
                                    Expressions.lambda(
                                            physType.record(expressionList), o_)));
        } else {
            convertedChildExp = childExp;
        }
        switch (getOperation()) {
            case UPDATE:
            case INSERT:
                builder.add(
                        Expressions.statement(
                                Expressions.call(
                                        collectionParameter,
                                        "addAll",
                                        Expressions.call(
                                                convertedChildExp,
                                                BuiltInMethod.INTO.method,
                                                Expressions.new_(ArrayList.class)))));
                break;
            case DELETE:
                builder.add(
                        Expressions.statement(
                                Expressions.call(
                                        convertedChildExp,
                                        BuiltInMethod.REMOVE_ALL.method,
                                        collectionParameter)));
                break;
            default:
                throw new AssertionError(getOperation());
        }
        final Expression updatedCountParameter =
                builder.append(
                        "updatedCount",
                        Expressions.call(collectionParameter, "size"),
                        false);
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                                Expressions.convert_(
                                        Expressions.condition(
                                                Expressions.greaterThanOrEqual(
                                                        updatedCountParameter, countParameter),
                                                Expressions.subtract(
                                                        updatedCountParameter, countParameter),
                                                Expressions.subtract(
                                                        countParameter, updatedCountParameter)),
                                        long.class))));
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref == Prefer.ARRAY
                                ? JavaRowFormat.ARRAY : JavaRowFormat.SCALAR);
        return implementor.result(physType, builder.toBlock());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Table modification is deliberately zero-cost because connector cost
        // is not visible to the planner.
        return planner.getCostFactory().makeZeroCost();
    }
}
