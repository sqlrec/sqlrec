package com.sqlrec;

import com.sqlrec.common.schema.SqlRecTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.prepare.Prepare.THREAD_EXPAND;
import static org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD;

public class TestCalciteSql {

    public static RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replace(root.collation)
                .simplify();
    }

    public static void main(String[] args) throws SqlParseException {
        // Define a schema (as in the previous example)
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("mySchema", new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new MyTable());
            }
        });

        // Configure SQL parser
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .build();

        // SQL query to parse
        String sql = "SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME";

        // Parse the SQL query
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();

        // Validate the SQL query
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                Collections.singletonList("mySchema"),
                new JavaTypeFactoryImpl(),
                null);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                Frameworks.newConfigBuilder().build().getOperatorTable(),
                catalogReader,
                new JavaTypeFactoryImpl(),
                SqlValidator.Config.DEFAULT
        );
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // Convert the SQL query to a relational expression
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        RelOptUtil.registerDefaultRules(planner, false, true);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(new JavaTypeFactoryImpl()));
        final SqlToRelConverter.Config config =
                SqlToRelConverter.config()
                        .withTrimUnusedFields(true)
                        .withExpand(THREAD_EXPAND.get())
                        .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()))
                        .withExplain(false);
        SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, queryString, schemaPath, viewPath) -> {
                    throw new UnsupportedOperationException("cannot expand view");
                },
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                config
        );
        RelRoot root = converter.convertQuery(validatedSqlNode, false, true);
        System.out.println(RelOptUtil.toString(root.rel));

        RelTraitSet desiredTraits = getDesiredRootTraitSet(root);
        Program program = Programs.standard();
        final RelNode bestExp =
                program.run(planner, root.rel, desiredTraits, new ArrayList<>(),
                        new ArrayList<>());
        System.out.println(RelOptUtil.toString(bestExp));

        Bindable bindable = EnumerableInterpretable.toBindable(
                new HashMap<>(),
                null,
                (EnumerableRel) bestExp,
                EnumerableRel.Prefer.ARRAY
        );
        Enumerable enumerable = bindable.bind(new DataContext() {
            @Override
            public @Nullable SchemaPlus getRootSchema() {
                return rootSchema;
            }

            @Override
            public JavaTypeFactory getTypeFactory() {
                return new JavaTypeFactoryImpl();
            }

            @Override
            public QueryProvider getQueryProvider() {
                return DEFAULT_PROVIDER;
            }

            @Override
            public @Nullable Object get(String name) {
                return null;
            }
        });

        List<Object[]> results = enumerable.toList();
        for (Object[] result : results) {
            System.out.println(result[0] + " " + result[1]);
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }

        @Override
        public SqlRecTableType getSqlRecTableType() {
            return SqlRecTableType.MEMORY;
        }
    }
}