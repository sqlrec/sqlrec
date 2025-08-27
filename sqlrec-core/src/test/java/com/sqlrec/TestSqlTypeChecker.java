package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.SqlTypeChecker;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.List;

public class TestSqlTypeChecker {
    public static void main(String[] args) throws Exception {
        List<String> sqlList = Arrays.asList(
                "use `default`",
                "select * from tt.t1",
                "select * from ( select * from tt.t1) t",
                "SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME",
                "select * from t1 union select * from t2 union select * from t3",
                "select * from t1 join t2 on t1.id = t2.id",
                "update t1 SET column1 = value1 where id = 1",
                "update t1 SET column1 = value1 where id in ( select id from t2)",
                "delete from t1 where id = 1",
                "delete from t1 where id in ( select id from t2)",
                "insert into t1 (id, column1) values (1, 'value1')"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            List<String> tableNames = SqlTypeChecker.getTableFromSqlNode(flinkSqlNode);
            System.out.println(tableNames);
        }
    }
}
