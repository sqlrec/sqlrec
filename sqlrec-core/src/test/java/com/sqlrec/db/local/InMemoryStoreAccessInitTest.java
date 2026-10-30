package com.sqlrec.db.local;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * InMemoryStoreAccess normalizes names when initialized from DDL nodes (the
 * file-based metadata mode), so mixed-case DDL still produces lower-case keys.
 */
class InMemoryStoreAccessInitTest {

    @Test
    void loadedFunctionsAndApisSupportInternalCrudAndRejectDuplicateInserts() throws Exception {
        var access = new InMemoryStoreAccess(
                Collections.singletonList(Collections.singletonList(CompileManager.parseSql("CREATE SQL FUNCTION f"))),
                Collections.singletonList(CompileManager.parseSql("CREATE API a WITH f")),
                Collections.emptyList());
        assertNotNull(access.getSqlFunction("f"));
        assertNotNull(access.getSqlApi("a"));
        var replacement = new SqlFunction();
        replacement.setName("f");
        replacement.setSqlList("[\"RETURN SELECT 2 AS id\"]");
        assertThrows(RuntimeException.class, () -> access.insertSqlFunction(replacement));
        access.upsertSqlFunction(replacement);
        assertEquals(replacement.getSqlList(), access.getSqlFunction("f").getSqlList());
        var api = new SqlApi();
        api.setName("a");
        api.setFunctionName("f");
        assertThrows(RuntimeException.class, () -> access.insertSqlApi(api));
        access.upsertSqlApi(api);
        assertEquals("f", access.getSqlApi("a").getFunctionName());
        var newApi = new SqlApi();
        newApi.setName("new_api");
        newApi.setFunctionName("new_function");
        access.insertSqlApi(newApi);
        var newFunction = new SqlFunction();
        newFunction.setName("new_function");
        access.insertSqlFunction(newFunction);
        access.deleteSqlApi("a");
        access.deleteSqlFunction("f");
        org.junit.jupiter.api.Assertions.assertNull(access.getSqlApi("a"));
        org.junit.jupiter.api.Assertions.assertNull(access.getSqlFunction("f"));
        assertNotNull(access.getSqlApi("new_api"));
        assertNotNull(access.getSqlFunction("new_function"));
    }

    @Test
    void initNormalizesNamesFromDdlNodes() throws Exception {
        SqlNode functionNode = CompileManager.parseSql("create sql function MyFunc");
        SqlNode apiNode = CompileManager.parseSql("create api MyApi with MyFunc");
        SqlNode modelNode = CompileManager.parseSql("create model MyModel (uid int)");

        InMemoryStoreAccess access = new InMemoryStoreAccess(
                Collections.singletonList(Collections.singletonList(functionNode)),
                Collections.singletonList(apiNode),
                Collections.singletonList(modelNode));

        SqlFunction sqlFunction = access.getSqlFunctionList().get(0);
        assertEquals("myfunc", sqlFunction.getName());
        assertNotNull(access.getSqlFunction("myfunc"));

        SqlApi sqlApi = access.getSqlApiList().get(0);
        assertEquals("myapi", sqlApi.getName());
        assertEquals("myfunc", sqlApi.getFunctionName());
        assertEquals(1, access.getSqlApiListByFunctionName("myfunc").size());

        Model model = access.getModelList().get(0);
        assertEquals("mymodel", model.getName());
    }
}
