package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestSqlParse {
    @Test
    public void testCalciteSql() throws Exception {
        List<String> sqlList = Arrays.asList(
                "set param=test",
                "set 'param'='test'",
                "use default",
                "show tables from default",
                "show tables in default",
                "call fun1(get('id'), t1, '10')",
                "call get('fun1')()",
                "call get('fun1')(get('id'), t1, '10')",
                "call fun1(get('id'), t1, '10') like t1",
                "call get('fun1')() like t1",
                "call get('fun1')(get('id'), t1, '10') like t1",
                "create sql function fun1",
                "create or replace sql function fun1",
                "create api api1 with fun1",
                "create or replace api api1 with fun1",
                "show sql functions",
                "show apis",
                "desc sql function fun1",
                "desc api api1",
                "cache table t2 as select * from t1 where id=1",
                "cache table t2 as call fun1(t1)",
                "call fun1(t1)",
                "define input table t1(id int, name string)",
                "return t1",
                "return"
        );

        for (String sql : sqlList) {
            SqlNode sqlNode = CompileManager.parseFlinkSql(sql);
            System.out.println(sqlNode.getClass());
            System.out.println(sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql());
        }
    }
}