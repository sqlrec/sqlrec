package com.sqlrec.compiler;

import com.sqlrec.rules.RuleManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.schema.RootFirstCatalogReader;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.prepare.Prepare.THREAD_EXPAND;
import static org.apache.calcite.prepare.Prepare.THREAD_INSUBQUERY_THRESHOLD;

public class NormalSqlCompiler {
    private static final Logger log = LoggerFactory.getLogger(NormalSqlCompiler.class);

    public static BindableInterface getNormalSqlBindable(String sql, CalciteSchema schema, String defaultSchema) throws Exception {
        log.info("compile sql: {}", sql);
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

        boolean containKvTable = SqlTypeChecker.isSqlContainKvTable(validatedSqlNode, schema, defaultSchema);

        // Convert the SQL query to a relational expression
        VolcanoPlanner planner = RuleManager.createPlanner(containKvTable);
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
        log.info("compile rel: {}", RelOptUtil.toString(root.rel));

        // todo use Programs.standard() when don't access outside storage
        Program program = getProgram();
        RelTraitSet desiredTraits = getDesiredRootTraitSet(root);
        final RelNode bestExp = program.run(planner, root.rel, desiredTraits, new ArrayList<>(), new ArrayList<>());
        log.info("compile bestExp: {}", RelOptUtil.toString(bestExp));

        Map<String, Object> parameters = new HashMap<>();
        Bindable bindable = EnumerableInterpretable.toBindable(
                parameters,
                null,
                (EnumerableRel) bestExp,
                EnumerableRel.Prefer.ARRAY
        );

        return new CalciteBindable(parameters, bindable, bestExp, sqlNode);
    }

    public static Program getProgram() {
        final Program program1 =
                (planner, rel, requiredOutputTraits, materializations, lattices) -> {
                    for (RelOptMaterialization materialization : materializations) {
                        planner.addMaterialization(materialization);
                    }
                    for (RelOptLattice lattice : lattices) {
                        planner.addLattice(lattice);
                    }

                    planner.setRoot(rel);
                    final RelNode rootRel2 =
                            rel.getTraitSet().equals(requiredOutputTraits)
                                    ? rel
                                    : planner.changeTraits(rel, requiredOutputTraits);
                    assert rootRel2 != null;

                    planner.setRoot(rootRel2);
                    final RelOptPlanner planner2 = planner.chooseDelegate();
                    final RelNode rootRel3 = planner2.findBestExp();
                    assert rootRel3 != null : "could not implement exp";
                    return rootRel3;
                };

        return Programs.sequence(Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
                program1,
                Programs.calc(DefaultRelMetadataProvider.INSTANCE));
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
