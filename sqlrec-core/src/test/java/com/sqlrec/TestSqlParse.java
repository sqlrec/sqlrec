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
                "call fun1(t1) async",
                "define input table t1(id integer, name string)",
                "return t1",
                "return",
                "create model `test_model` (id bigint, name string) with ('param'='value')",
                "create model if not exists `test_model` (id bigint, name string, price float) with ('param1'='value1', 'param2'='value2')",
                "drop model `test_model`",
                "drop model if exists `test_model`",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table from 'checkpoint_path1'",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01' and dt < '2023-02-01'",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01' and dt < '2023-02-01' WITH ( 'param1' = 'value1', 'param2' = 'value2' )",
                "show models",
                "describe model test_model",
                "describe model test_model",
                "show checkpoints test_model",
                "describe checkpoint test_model.checkpoint1",
                "describe checkpoint test_model.checkpoint1"
        );

        for (String sql : sqlList) {
            SqlNode sqlNode = CompileManager.parseFlinkSql(sql);
            System.out.println(sqlNode.getClass());
            System.out.println(sql);
            System.out.println(sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql());
            assert getPlainSql(sql).equals(getPlainSql(sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql()));
        }
    }

    public static String getPlainSql(String sql) {
        return sql.replaceAll("'", "")
                .replaceAll("`", "")
                .replaceAll(" ", "")
                .replaceAll("\n", "")
                .replaceAll("\r", "")
                .toUpperCase();
    }
}