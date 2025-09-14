package com.sqlrec.compiler;

import com.sqlrec.rules.RuleManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.schema.RootFirstCatalogReader;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.*;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.prepare.Prepare.THREAD_EXPAND;
import static org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD;

public class NormalSqlCompiler {
    public static String DEFAULT_SCHEMA_NAME = "default";

    public static BindableInterface getNormalSqlBindable(String sql, CalciteSchema schema, String defaultSchema) throws Exception {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .build();

        // Parse the SQL query
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();

        // Validate the SQL query
        CalciteCatalogReader catalogReader = getCatalogReader(schema, defaultSchema);
        SqlValidator validator = createSqlValidate(schema, defaultSchema);
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // Convert the SQL query to a relational expression
        VolcanoPlanner planner = RuleManager.createPlanner();
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
        final RelNode bestExp = program.run(planner, root.rel, desiredTraits, new ArrayList<>(), new ArrayList<>());
        System.out.println(RelOptUtil.toString(bestExp));

        Map<String, Object> parameters = new HashMap<>();
        Bindable bindable = EnumerableInterpretable.toBindable(
                parameters,
                null,
                (EnumerableRel) bestExp,
                EnumerableRel.Prefer.ARRAY
        );

        return new CalciteBindable(parameters, bindable, bestExp);
    }

    public static CalciteCatalogReader getCatalogReader(CalciteSchema schema, String defaultSchema) {
        return new RootFirstCatalogReader(
                schema,
                Collections.singletonList(defaultSchema),
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                null);
    }

    public static SqlValidator createSqlValidate(CalciteSchema schema, String defaultSchema) {
        CalciteCatalogReader catalogReader = getCatalogReader(schema, defaultSchema);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                getOperatorTable(catalogReader),
                catalogReader,
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                SqlValidator.Config.DEFAULT
        );
        return validator;
    }

    public static SqlOperatorTable getOperatorTable(CalciteCatalogReader catalogReader) {
        final List<SqlOperatorTable> list = new ArrayList<>();
        list.add(SqlStdOperatorTable.instance());
        list.add(catalogReader);
        return SqlOperatorTables.chain(list);
    }

    private static RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replace(root.collation)
                .simplify();
    }
}
