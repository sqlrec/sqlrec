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
                "call fun1(get('id'), t1, '10') like function 'fun2'",
                "call get('fun1')() like function 'fun2'",
                "call get('fun1')(get('id'), t1, '10') like function 'fun2'",
                "call fun1(t1) like function 'fun2' async",
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
                "alter model test_model drop checkpoint='checkpoint1'",
                "alter model test_model drop if exists checkpoint='checkpoint1'",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table from 'checkpoint_path1'",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01' and dt < '2023-02-01'",
                "train model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01' and dt < '2023-02-01' WITH ( 'param1' = 'value1', 'param2' = 'value2' )",
                "show models",
                "show checkpoints test_model",
                "describe model test_model",
                "describe model test_model checkpoint='checkpoint1'",
                "describe formatted model test_model",
                "describe formatted model test_model checkpoint='checkpoint1'",
                "export model test_model checkpoint='checkpoint_path'",
                "export model test_model checkpoint='checkpoint_path' on data_db.test_table where dt>='2023-01-01'",
                "export model test_model checkpoint='checkpoint_path' WITH ( 'param1' = 'value1', 'param2' = 'value2' )",
                "create service my_service on model test_model",
                "create service my_service on model test_model checkpoint='checkpoint_path'",
                "create service my_service on model test_model with ('param1'='value1')",
                "create service my_service on model test_model checkpoint='checkpoint_path' with ('param1'='value1', 'param2'='value2')",
                "create service if not exists my_service on model test_model",
                "create service if not exists my_service on model test_model checkpoint='checkpoint_path'",
                "show services",
                "describe service my_service",
                "describe formatted service my_service",
                "drop service my_service",
                "drop service if exists my_service",
                "drop sql function fun1",
                "drop sql function if exists fun1",
                "drop api api1",
                "drop api if exists api1",
                "if (select * from t1) then (cache table t2 as select * from t1)",
                "if timein (select * from t1 where id=1) then (cache table t2 as select * from t1)",
                "if (select * from t1) then (cache table t2 as select * from t1) else (cache table t3 as select * from t2)",
                "if timein (select * from t1 where id=1) then (cache table t2 as select * from t1) else (cache table t3 as select * from t2)",
                "if (select * from t1) then (cache table t2 as call fun1(t1))",
                "if (select * from t1) then (cache table t2 as call get('fun1')(get('id'), t1, '10') like function 'fun2')",
                "if timein (select * from t1) then (cache table t2 as call fun1(t1)) else (cache table t3 as call fun2(t2))",
                "IF (SELECT count(*) > 0 FROM input1) THEN (cache table result1 as SELECT * FROM input1) ELSE (cache table result1 as SELECT 0 as id, 'empty' as name)"
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