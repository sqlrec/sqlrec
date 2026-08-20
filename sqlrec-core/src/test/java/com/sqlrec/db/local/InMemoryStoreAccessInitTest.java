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

/**
 * InMemoryStoreAccess normalizes names when initialized from DDL nodes (the
 * file-based metadata mode), so mixed-case DDL still produces lower-case keys.
 */
class InMemoryStoreAccessInitTest {

    @Test
    void initNormalizesNamesFromDdlNodes() throws Exception {
        SqlNode functionNode = CompileManager.parseFlinkSql("create sql function MyFunc");
        SqlNode apiNode = CompileManager.parseFlinkSql("create api MyApi with MyFunc");
        SqlNode modelNode = CompileManager.parseFlinkSql("create model MyModel (uid int)");

        InMemoryStoreAccess access = new InMemoryStoreAccess(
                Collections.singletonList(Collections.singletonList(functionNode)),
                Collections.singletonList(apiNode),
                Collections.singletonList(modelNode)
        );

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
