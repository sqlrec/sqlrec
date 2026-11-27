package com.sqlrec.node;

import com.sqlrec.common.schema.SqlRecCollection;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Linq4j;
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

public class SqlrecEnumerableTableModify extends EnumerableTableModify {
    public static void replaceRows(Collection<Object[]> collection,
                                   Collection<Object[]> oldRows, Collection<Object[]> newRows) {
        if (oldRows.size() != newRows.size()) {
            throw new IllegalArgumentException("UPDATE row counts do not match");
        }
        if (collection instanceof SqlRecCollection tableRows) {
            tableRows.replaceAll(new ArrayList<>(oldRows), new ArrayList<>(newRows));
        } else {
            collection.addAll(newRows);
        }
    }

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
        BlockBuilder builder = new BlockBuilder();
        Result input = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        Expression inputRows = builder.append("child", input.block);
        Expression collection = declareCollection(builder);
        Expression sizeBefore = builder.append("sizeBefore", Expressions.call(collection, "size"), false);

        Expression modification = modifyRows(builder, collection, inputRows, input.physType);
        builder.add(Expressions.statement(modification));

        Expression sizeChange = Expressions.subtract(Expressions.call(collection, "size"), sizeBefore);
        Expression affectedRows = Expressions.convert_(Expressions.call(Math.class, "abs", sizeChange), long.class);
        builder.add(Expressions.return_(null,
                Expressions.call(BuiltInMethod.SINGLETON_ENUMERABLE.method, affectedRows)));
        PhysType resultType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
                pref == Prefer.ARRAY ? JavaRowFormat.ARRAY : JavaRowFormat.SCALAR);
        return implementor.result(resultType, builder.toBlock());
    }

    private ParameterExpression declareCollection(BlockBuilder builder) {
        Expression tableExpression = table.getExpression(ModifiableTable.class);
        if (tableExpression == null) {
            throw new IllegalStateException("ModifiableTable expression is missing");
        }
        if (!ModifiableTable.class.isAssignableFrom(Types.toClass(tableExpression.getType()))) {
            throw new IllegalStateException(
                    "Expected a ModifiableTable expression, got " + tableExpression.getType());
        }
        ParameterExpression collection = Expressions.parameter(Collection.class, builder.newName("collection"));
        Expression getCollection = Expressions.call(tableExpression,
                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);
        builder.add(Expressions.declare(Modifier.FINAL, collection, getCollection));
        return collection;
    }

    private Expression modifyRows(
            BlockBuilder builder, Expression collection, Expression inputRows, PhysType inputType) {
        JavaTypeFactory typeFactory = (JavaTypeFactory) getCluster().getTypeFactory();
        JavaRowFormat format = EnumerableTableScan.deduceFormat(table);
        // Tables backed by Object[] require an array even when the row has a single field.
        PhysType tableType = PhysTypeImpl.of(
                typeFactory, table.getRowType(), format, format != JavaRowFormat.ARRAY);
        int fieldCount = inputType.getRowType().getFieldCount();
        if (getOperation() == Operation.UPDATE) {
            List<String> updateColumns = getUpdateColumnList();
            if (updateColumns == null) {
                throw new IllegalStateException("UPDATE columns are missing");
            }
            // UPDATE input contains the original row followed by SET values.
            int tableFieldCount = fieldCount - updateColumns.size();
            // Materialize the input once so both projections see the same rows and SET values.
            Expression inputList = builder.append("updateInputRows", collectRows(inputRows));
            Expression source = Expressions.call(Linq4j.class, "asEnumerable", inputList);
            Expression oldRows = projectRows(source, inputType, tableType, tableFieldCount, List.of());
            Expression newRows = projectRows(source, inputType, tableType, tableFieldCount, updateColumns);
            return Expressions.call(SqlrecEnumerableTableModify.class, "replaceRows",
                    collection, collectRows(oldRows), collectRows(newRows));
        }

        Expression rows = projectRows(inputRows, inputType, tableType, fieldCount, List.of());
        return switch (getOperation()) {
            case INSERT -> Expressions.call(collection, "addAll", collectRows(rows));
            case DELETE -> Expressions.call(rows, BuiltInMethod.REMOVE_ALL.method, collection);
            default -> throw new AssertionError(getOperation());
        };
    }

    private Expression projectRows(Expression source, PhysType inputType, PhysType tableType,
                                   int fieldCount, List<String> updateColumns) {
        ParameterExpression row = Expressions.parameter(inputType.getJavaRowType(), "row");
        var inputFields = getInput().getRowType().getFieldList();
        List<Expression> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            int updateIndex = updateColumns.isEmpty() ? -1 : updateColumns.indexOf(inputFields.get(i).getName());
            int inputIndex = updateIndex < 0 ? i : fieldCount + updateIndex;
            fields.add(inputType.fieldReference(row, inputIndex, tableType.getJavaFieldType(i)));
        }
        return Expressions.call(source, BuiltInMethod.SELECT.method,
                Expressions.lambda(tableType.record(fields), row));
    }

    private static Expression collectRows(Expression rows) {
        return Expressions.call(rows, BuiltInMethod.INTO.method, Expressions.new_(ArrayList.class));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Table modification is deliberately zero-cost because connector cost
        // is not visible to the planner.
        return planner.getCostFactory().makeZeroCost();
    }
}
