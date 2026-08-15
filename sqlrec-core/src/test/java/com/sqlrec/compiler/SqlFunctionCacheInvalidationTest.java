package com.sqlrec.compiler;

import com.sqlrec.common.config.Consts;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import com.sqlrec.db.local.SqlFileSchemaAccess;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verify that compiled sql function cache is refreshed immediately after the
 * function is updated / dropped, without waiting for the periodic FunctionUpdater.
 */
class SqlFunctionCacheInvalidationTest {
    private static final String[] TEST_FUNCTIONS = {
            "FUN_UPD", "FUN_DEP_A", "FUN_DEP_B", "FUN_DROP"
    };

    private MetadataAccess savedMetadataAccess;

    @BeforeEach
    void setUp() throws Exception {
        CalciteSchema globalSchema = CalciteSchema.createRootSchema(false);
        globalSchema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.emptyMap();
            }
        });
        CalciteSchemaFactory.setGlobalSchema(globalSchema);
        JavaFunctionUtils.setSkipHmsQuery(true);
        CompileManager.invalidateCache();

        savedMetadataAccess = (MetadataAccess) getStaticField("instance");
        setStaticField("instance", new MetadataAccess(
                new SqlFileSchemaAccess(new ArrayList<>(), new ArrayList<>()),
                new InMemoryStoreAccess(new ArrayList<>(), new ArrayList<>(), new ArrayList<>()),
                new LocalHdfsAccess()
        ));
    }

    @AfterEach
    void tearDown() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        for (String functionName : TEST_FUNCTIONS) {
            db.deleteSqlFunction(functionName);
        }
        CompileManager.invalidateCache();
        CalciteSchemaFactory.setGlobalSchema(null);
        JavaFunctionUtils.setSkipHmsQuery(false);
        setStaticField("instance", savedMetadataAccess);
    }

    @Test
    void updateSqlFunctionTakesEffectImmediately() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        createFunction(executor, false, "fun_upd", 1);
        assertCallResult(executor, "call fun_upd()", 1); // warm up the compile cache

        createFunction(executor, true, "fun_upd", 2); // create or replace
        assertCallResult(executor, "call fun_upd()", 2); // must see the new version immediately
    }

    @Test
    void dependentFunctionSeesUpdatedCalleeImmediately() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        createFunction(executor, false, "fun_dep_a", 1);

        executor.executeSqlAsync("create sql function fun_dep_b");
        executor.executeSqlAsync("cache table t as call fun_dep_a()");
        executor.executeSqlAsync("return t");

        assertCallResult(executor, "call fun_dep_b()", 1); // both functions now cached

        createFunction(executor, true, "fun_dep_a", 2);
        // dependent function must not keep the embedded old callee version
        assertCallResult(executor, "call fun_dep_b()", 2);
    }

    @Test
    void dropSqlFunctionTakesEffectImmediately() throws Exception {
        SqlExecutor executor = new SqlExecutor();
        createFunction(executor, false, "fun_drop", 1);
        assertCallResult(executor, "call fun_drop()", 1); // warm up the compile cache

        executor.executeSqlAsync("drop sql function fun_drop");

        Exception e = assertThrows(Exception.class, () -> executor.executeSqlAsync("call fun_drop()"));
        assertTrue(e.getMessage().contains("function not fund"));
    }

    @Test
    void cacheTableCallDependencyIsCollected() throws Exception {
        CompileManager compileManager = new CompileManager();
        compileManager.compileSqlFunction("fun_dep_a", Arrays.asList(
                "create sql function fun_dep_a",
                "cache table r as select 1 as a",
                "return r"));
        SqlFunctionBindable funDepB = compileManager.compileSqlFunction("fun_dep_b", Arrays.asList(
                "create sql function fun_dep_b",
                "cache table t as call fun_dep_a()",
                "return t"));

        assertTrue(funDepB.getDependencySqlFunctions().contains("FUN_DEP_A"));
    }

    @Test
    void ifCacheDependencyIsCollected() throws Exception {
        CompileManager compileManager = new CompileManager();
        compileManager.compileSqlFunction("fun_if_a", Arrays.asList(
                "create sql function fun_if_a",
                "cache table r as select 1 as a",
                "return r"));
        compileManager.compileSqlFunction("fun_if_c", Arrays.asList(
                "create sql function fun_if_c",
                "cache table r as select 3 as a",
                "return r"));
        SqlFunctionBindable funIfB = compileManager.compileSqlFunction("fun_if_b", Arrays.asList(
                "create sql function fun_if_b",
                "if (select true) then (cache table t as call fun_if_a()) else (cache table t as call fun_if_c())",
                "return t"));

        Set<String> dependencies = funIfB.getDependencySqlFunctions();
        assertTrue(dependencies.contains("FUN_IF_A"));
        assertTrue(dependencies.contains("FUN_IF_C"));
    }

    private void createFunction(SqlExecutor executor, boolean orReplace, String name, int value) throws Exception {
        executor.executeSqlAsync((orReplace ? "create or replace sql function " : "create sql function ") + name);
        executor.executeSqlAsync("cache table r as select " + value + " as a");
        executor.executeSqlAsync("return r");
    }

    private void assertCallResult(SqlExecutor executor, String sql, Object expectedValue) throws Exception {
        SqlProcessResult result = executor.executeSqlAsync(sql);
        Enumerable<Object[]> enumerable = result.getEnumerable();
        assertNotNull(enumerable, "call sql should return result rows");
        List<Object[]> rows = enumerable.toList();
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).length);
        assertEquals(expectedValue, rows.get(0)[0]);
    }

    private static Object getStaticField(String fieldName) throws Exception {
        Field field = MetadataAccessFactory.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }

    private static void setStaticField(String fieldName, Object value) throws Exception {
        Field field = MetadataAccessFactory.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
