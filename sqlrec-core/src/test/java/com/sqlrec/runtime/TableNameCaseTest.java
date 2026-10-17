package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.ConcurrentCalciteSchema;
import com.sqlrec.schema.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TableNameCaseTest {
    private boolean originalParallelism;

    @BeforeEach
    void setUp() {
        originalParallelism = SqlRecConfigs.PARALLELISM_EXEC.getDefaultValue();
        SqlFunctionCache.invalidateAll();
        CalciteSchema global = ConcurrentCalciteSchema.createRootSchema();
        global.add("CaseDb", new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
                return Map.of("MiXeD_Source", new CacheTable("MiXeD_Source",
                        Linq4j.singletonEnumerable(new Object[]{9}),
                        List.of(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER))));
            }
        });
        CalciteSchemaFactory.setGlobalSchema(global);
        JavaFunctionUtils.setSkipHmsQuery(true);
        JavaFunctionUtils.registerTableFunction("default", "case_echo", Echo.class);
    }

    @AfterEach
    void tearDown() {
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(originalParallelism);
        SqlFunctionCache.invalidateAll();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @Test
    void quotedTablesAndAliasesWorkWithoutChangingColumnsOrLiterals() throws Exception {
        SqlFunctionBindable function = compile("case_literal",
                "CACHE TABLE `MiXeD_Table` AS SELECT 'KeepCase' AS `DisplayName`, 7 AS `ValueID`",
                "RETURN SELECT A.`DisplayName`, A.`ValueID` FROM `MIXED_TABLE` AS A");
        assertEquals(List.of("DisplayName", "ValueID"), function.getReturnDataFields().stream()
                .map(field -> field.getName()).toList());
        for (boolean parallel : List.of(false, true)) {
            assertArrayEquals(new Object[]{"KeepCase", 7}, execute(function, parallel).getFirst());
        }
    }

    @Test
    void javaCallsLikeAndPartitionAcceptMixedCaseReferences() throws Exception {
        SqlFunctionBindable function = compile("case_java",
                "CACHE TABLE Seed_Table AS SELECT 7 AS id",
                "CACHE TABLE Java_Result AS CALL case_echo(SEED_TABLE) LIKE sEeD_tAbLe",
                "CACHE TABLE Dynamic_Result AS CALL GET_OR_DEFAULT('callee', 'case_echo')(sEeD_tAbLe) "
                        + "LIKE SEED_TABLE PARTITION BY Seed_Table SIZE 1",
                "RETURN SELECT * FROM JAVA_RESULT UNION ALL SELECT * FROM dynamic_result");
        assertInts(function, List.of(7, 7));
    }

    @Test
    void sqlCallsAndBareReturnsAcceptMixedCaseReferences() throws Exception {
        compile("case_child", "DEFINE INPUT TABLE Input_Table(id integer)", "RETURN INPUT_TABLE");
        SqlFunctionBindable function = compile("case_parent",
                "CACHE TABLE Seed_Table AS SELECT 7 AS id",
                "CACHE TABLE Static_Result AS CALL case_child(SEED_TABLE)",
                "CACHE TABLE Dynamic_Result AS CALL GET_OR_DEFAULT('callee', 'case_child')(sEeD_tAbLe) LIKE SEED_TABLE",
                "RETURN SELECT * FROM STATIC_RESULT UNION ALL SELECT * FROM dynamic_result");
        assertInts(function, List.of(7, 7));
    }

    @Test
    void externalDatabaseAndTableReferencesResolveTheirActualNames() throws Exception {
        for (String table : List.of("CASEDB.MIXED_SOURCE", "`cAsEdB`.`mIxEd_Source`")) {
            assertInts(compile("case_external", "RETURN SELECT S.ID FROM " + table + " AS S"), List.of(9));
        }
    }

    @Test
    void inputDeclarationsDifferingOnlyInCaseMustBeRejected() {
        for (String duplicate : List.of("Input_Table", "INPUT_TABLE")) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> compile("case_duplicate",
                            "DEFINE INPUT TABLE Input_Table(id integer)",
                            "DEFINE INPUT TABLE " + duplicate + "(id integer)",
                            "RETURN input_table"));
            assertEquals("duplicate input table: input_table", failure.getMessage());
        }
    }

    @Test
    void quotedTableNameSpacesRemainPartOfTheIdentifier() throws Exception {
        assertInts(compile("case_spaces",
                "CACHE TABLE My_Table AS SELECT 1 AS id",
                "CACHE TABLE ` My_Table ` AS SELECT 7 AS id",
                "RETURN SELECT * FROM MY_TABLE UNION ALL SELECT * FROM ` my_table `"), List.of(1, 7));
    }

    private void assertInts(SqlFunctionBindable function, List<Integer> expected) {
        for (boolean parallel : List.of(false, true)) {
            assertEquals(expected, execute(function, parallel).stream().map(row -> (Integer) row[0]).toList());
        }
    }

    private List<Object[]> execute(SqlFunctionBindable function, boolean parallel) {
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        SqlRecConfigs.PARALLELISM_EXEC.setDefaultValue(parallel);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable(SqlRecConfigs.IGNORE_UNION_EXCEPTION.getKey(), "false");
        return function.bind(schema, context).toList();
    }

    private SqlFunctionBindable compile(String name, String... body) throws Exception {
        List<String> statements = new ArrayList<>();
        statements.add("CREATE SQL FUNCTION " + name);
        statements.addAll(List.of(body));
        return new CompileManager().compileSqlFunction(name, statements);
    }

    public static class Echo {
        public CacheTable evaluate(CacheTable input) { return input; }
    }
}
