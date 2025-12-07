package com.sqlrec.node;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

public class FilterableTableScan extends EnumerableTableScan {
    private final Class elementType;
    public final ImmutableList<RexNode> filters;
    public final ImmutableIntList projects;

    /**
     * Creates an EnumerableTableScan.
     *
     * <p>Use {@link #create} unless you know what you are doing.
     *
     * @param cluster
     * @param traitSet
     * @param table
     * @param elementType
     */
    public FilterableTableScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, Class elementType,
            ImmutableList<RexNode> filters, ImmutableIntList projects) {
        super(cluster, traitSet, table, elementType);
        this.elementType = elementType;
        this.filters = filters;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FilterableTableScan(
                getCluster(),
                traitSet,
                table,
                elementType,
                filters,
                projects
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("filters", filters, !filters.isEmpty())
                .itemIf("projects", projects, !projects.equals(identity()));
    }

    public static FilterableTableScan create(
            RelOptCluster cluster,
            RelOptTable relOptTable,
            ImmutableList<RexNode> filters,
            ImmutableIntList projects
    ) {
        final Table table = relOptTable.unwrap(Table.class);
        Class elementType = FilterableTableScan.deduceElementType(table);
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new FilterableTableScan(cluster, traitSet, relOptTable, elementType, filters, projects);
    }


    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        FilterableTable sqlRecTable = table.unwrap(FilterableTable.class);
        if (sqlRecTable == null) {
            return super.implement(implementor, pref);
        }

        RelOptSchema relOptSchema = table.getRelOptSchema();
        if (!(relOptSchema instanceof CalciteCatalogReader)) {
            throw new IllegalArgumentException("FilterableTableScan only support CalciteCatalogReader");
        }

        CalciteCatalogReader catalogReader = (CalciteCatalogReader) relOptSchema;
        List<String> qualifiedName = table.getQualifiedName();
        CalciteSchema.TableEntry tableEntry = SqlValidatorUtil.getTableEntry(catalogReader, qualifiedName);
        if (tableEntry == null) {
            throw new IllegalArgumentException("Table not found: " + qualifiedName);
        }

        Expression expression = Expressions.call(
                Schemas.expression(tableEntry.schema.plus()),
                BuiltInMethod.SCHEMA_GET_TABLE.method,
                Expressions.constant(tableEntry.name));
        expression = Expressions.convert_(expression, FilterableTable.class);


        final Expression stashedFilters = implementor.stash(filters, List.class);
        final Expression scanCall =
                Expressions.call(expression, "scan",
                        implementor.getRootExpression(), stashedFilters);
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.preferArray(),
                false
        );

        return implementor.result(physType, Blocks.toBlock(scanCall));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }
}
