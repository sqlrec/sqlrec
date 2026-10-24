package com.sqlrec.udf;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.calcite.DataContexts;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NonDeterministicScalarFunctionTest {
    private CalciteSchema schema;

    @BeforeEach
    void setup() throws Exception {
        schema = CalciteSchema.createRootSchema(false);
        CalciteSchema child = schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        for (String name : Arrays.asList("uuid", "random_vec", "l2_norm")) {
            UdfManager.addFunction(child, name, FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get(name));
        }
        CalciteSchemaFactory.setGlobalSchema(schema);
    }

    @AfterEach
    void cleanup() {
        CalciteSchemaFactory.setGlobalSchema(null);
    }

    @Test
    void hiveDeclarationsReachCalciteWithoutChangingDeterministicFunctions() {
        assertFalse(operator("uuid").isDeterministic());
        assertFalse(operator("random_vec").isDeterministic());
        assertTrue(operator("l2_norm").isDeterministic());
    }

    @Test
    void constantReductionRetainsUuidAndRandomVectorCalls() {
        RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build())
                .transform(config -> config.withSimplify(false));
        Project project = (Project) builder.values(new String[]{"n"}, 1, 2, 3)
                .project(builder.call(operator("uuid")),
                        builder.call(operator("random_vec"), builder.literal("4")),
                        builder.call(SqlStdOperatorTable.ABS, builder.literal(-1)))
                .build();
        HepPlanner planner = new HepPlanner(new HepProgramBuilder()
                .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS).build());
        RexExecutorImpl executor = new RexExecutorImpl(DataContexts.EMPTY);
        project.getCluster().getPlanner().setExecutor(executor);
        planner.setExecutor(executor);
        assertInstanceOf(RexCall.class, project.getProjects().get(2));
        planner.setRoot(project);
        Project optimized = (Project) planner.findBestExp();

        assertInstanceOf(RexCall.class, optimized.getProjects().get(0));
        assertInstanceOf(RexCall.class, optimized.getProjects().get(1));
        assertEquals("uuid", ((RexCall) optimized.getProjects().get(0)).getOperator().getName());
        assertEquals("random_vec", ((RexCall) optimized.getProjects().get(1)).getOperator().getName());
        // This control proves that the reduction rule actually evaluated constant expressions.
        RexNode reduced = RexUtil.removeCast(optimized.getProjects().get(2));
        assertInstanceOf(RexLiteral.class, reduced, reduced.toString());
        assertEquals(1, RexLiteral.intValue(reduced));
    }

    @Test
    void constantArgumentsProduceFreshResultsPerRowAndAcrossPlanExecutions() throws Exception {
        BindableInterface plan = compile("select uuid(), random_vec('4') "
                + "from (values (1), (2), (3)) as t(n)");
        Set<String> identifiers = new HashSet<>();
        Set<List<Double>> vectors = new HashSet<>();
        for (int run = 0; run < 2; run++) {
            List<Object[]> rows = plan.bind(schema, new ExecuteContextImpl(schema)).toList();
            assertEquals(3, rows.size());
            for (Object[] row : rows) {
                String identifier = (String) row[0];
                assertDoesNotThrow(() -> UUID.fromString(identifier));
                assertTrue(identifiers.add(identifier), "uuid must be fresh for each row and execution");
                List<Double> vector = vector(row[1]);
                assertTrue(vectors.add(vector), "random_vec must be fresh for each row and execution");
            }
        }
        assertEquals(6, identifiers.size());
        assertEquals(6, vectors.size());
    }

    private List<Double> vector(Object value) {
        assertInstanceOf(List.class, value);
        List<?> values = (List<?>) value;
        assertEquals(4, values.size());
        List<Double> result = new ArrayList<>();
        for (Object item : values) {
            assertInstanceOf(Double.class, item);
            assertTrue(Double.isFinite((Double) item));
            result.add((Double) item);
        }
        assertEquals(1.0, result.stream().mapToDouble(number -> number * number).sum(), 1e-10);
        return result;
    }

    private SqlOperator operator(String name) {
        List<SqlOperator> operators = new ArrayList<>();
        NormalSqlCompiler.getCatalogReader(schema, Consts.DEFAULT_SCHEMA_NAME).lookupOperatorOverloads(
                new SqlIdentifier(name, SqlParserPos.ZERO), SqlFunctionCategory.USER_DEFINED_FUNCTION,
                SqlSyntax.FUNCTION, operators, SqlNameMatchers.withCaseSensitive(false));
        assertEquals(1, operators.size());
        return operators.get(0);
    }

    private BindableInterface compile(String sql) throws Exception {
        return new CompileManager().compileSql(CompileManager.parseSql(sql), schema, Consts.DEFAULT_SCHEMA_NAME, sql);
    }
}
