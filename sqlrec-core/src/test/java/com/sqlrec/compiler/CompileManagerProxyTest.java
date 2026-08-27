package com.sqlrec.compiler;

import com.sqlrec.common.config.Consts;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.IfBindable;
import com.sqlrec.runtime.ProxyAllBindable;
import com.sqlrec.runtime.SetBindable;
import com.sqlrec.runtime.SqlFunctionBindable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class CompileManagerProxyTest {

    @Test
    void compileSqlReturnsAProxyAroundTheRawBindable() throws Exception {
        String sql = "set proxy_key=proxy_value";
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        BindableInterface bindable = new CompileManager().compileSql(
                sqlNode,
                CalciteSchema.createRootSchema(false),
                Consts.DEFAULT_SCHEMA_NAME,
                sql
        );

        ProxyAllBindable proxy = assertInstanceOf(ProxyAllBindable.class, bindable);
        assertInstanceOf(SetBindable.class, proxy.getDelegate());
        assertFalse(proxy.getDelegate() instanceof ProxyAllBindable);
        assertEquals(sql, bindable.getSql());
        assertEquals("set", bindable.getName());
    }

    @Test
    void compileSqlKeepsIfBranchesRawForConcreteTypeValidation() throws Exception {
        String sql = "IF (SELECT true) "
                + "THEN (cache table proxy_result as SELECT 1 as id) "
                + "ELSE (cache table proxy_result as SELECT 2 as id)";
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        BindableInterface bindable = new CompileManager().compileSql(
                sqlNode,
                CalciteSchema.createRootSchema(false),
                Consts.DEFAULT_SCHEMA_NAME,
                sql
        );

        ProxyAllBindable proxy = assertInstanceOf(ProxyAllBindable.class, bindable);
        IfBindable ifBindable = assertInstanceOf(IfBindable.class, proxy.getDelegate());
        assertInstanceOf(CacheTableBindable.class, ifBindable.getThenClause());
        assertInstanceOf(CacheTableBindable.class, ifBindable.getElseClause());
        assertFalse(ifBindable.getThenClause() instanceof ProxyAllBindable);
        assertFalse(ifBindable.getElseClause() instanceof ProxyAllBindable);
        assertEquals("proxy_result", bindable.getName());
    }

    @Test
    void functionBodyContainsExactlyOneProxyPerCompiledStatement() throws Exception {
        FunctionCompiler compiler = new FunctionCompiler(
                CalciteSchema.createRootSchema(false),
                new CompileManager()
        );
        compiler.compileAllSql(Arrays.asList(
                "create sql function proxy_test",
                "set proxy_key=proxy_value",
                "return"
        ));
        SqlFunctionBindable function = compiler.getFunctionBindable();

        assertEquals(1, function.getBindableList().size());
        ProxyAllBindable statement = assertInstanceOf(
                ProxyAllBindable.class,
                function.getBindableList().get(0)
        );
        assertInstanceOf(SetBindable.class, statement.getDelegate());
        assertFalse(statement.getDelegate() instanceof ProxyAllBindable);
        assertEquals("proxy_test:set:1", statement.getName());

        BindableInterface executableFunction = CompileManager.prepareSqlFunctionForExecution(function);
        ProxyAllBindable functionProxy = assertInstanceOf(ProxyAllBindable.class, executableFunction);
        assertSame(function, functionProxy.getDelegate());
        assertEquals("proxy_test", executableFunction.getName());
    }
}
