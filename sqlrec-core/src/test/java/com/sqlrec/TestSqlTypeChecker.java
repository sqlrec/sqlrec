package com.sqlrec;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSqlTypeChecker {

    private static class SqlTestCase {
        String sql;
        List<String> expectedTables;
        List<String> expectedModifyTables;

        SqlTestCase(String sql, List<String> expectedTables, List<String> expectedModifyTables) {
            this.sql = sql;
            this.expectedTables = expectedTables;
            this.expectedModifyTables = expectedModifyTables;
        }
    }

    @Test
    public void testSqlTypeChecker() throws Exception {
        List<SqlTestCase> testCases = Arrays.asList(
                new SqlTestCase("use `default`", Arrays.asList(), Arrays.asList()),
                new SqlTestCase("select * from db1.t1", Arrays.asList("db1.t1"), Arrays.asList()),
                new SqlTestCase("select * from ( select * from db1.t1) t", Arrays.asList("db1.t1"), Arrays.asList()),
                new SqlTestCase("SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME", Arrays.asList("myTable"), Arrays.asList()),
                new SqlTestCase("select * from t1 union select * from t2 union select * from t3", Arrays.asList("t1", "t2", "t3"), Arrays.asList()),
                new SqlTestCase("select * from t1 join t2 on t1.id = t2.id", Arrays.asList("t1", "t2"), Arrays.asList()),
                new SqlTestCase("update t1 SET column1 = value1 where id = 1", Arrays.asList("t1"), Arrays.asList("t1")),
                new SqlTestCase("update t1 SET column1 = value1 where id in ( select id from t2)", Arrays.asList("t1", "t2"), Arrays.asList("t1")),
                new SqlTestCase("delete from t1 where id = 1", Arrays.asList("t1"), Arrays.asList("t1")),
                new SqlTestCase("delete from t1 where id in ( select id from t2)", Arrays.asList("t1", "t2"), Arrays.asList("t1")),
                new SqlTestCase("insert into t1 (id, column1) values (1, 'value1')", Arrays.asList("t1"), Arrays.asList("t1")),
                new SqlTestCase("insert into t1 (id, column1) select id, column1 from t2", Arrays.asList("t1", "t2"), Arrays.asList("t1")),
                new SqlTestCase("insert into db1.t1 (id, column1) select id, column1 from db1.t2", Arrays.asList("db1.t1", "db1.t2"), Arrays.asList("db1.t1"))
        );

        for (SqlTestCase testCase : testCases) {
            System.out.println("\nTesting: " + testCase.sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(testCase.sql);

            List<String> actualTables = NodeUtils.getTableFromSqlNode(flinkSqlNode);
            System.out.println("Expected tables: " + testCase.expectedTables);
            System.out.println("Actual tables: " + actualTables);
            assertEquals(testCase.expectedTables, actualTables, "Tables mismatch for SQL: " + testCase.sql);

            List<String> actualModifyTables = NodeUtils.getModifyTablesFromSqlNode(flinkSqlNode);
            System.out.println("Expected modify tables: " + testCase.expectedModifyTables);
            System.out.println("Actual modify tables: " + actualModifyTables);
            assertEquals(testCase.expectedModifyTables, actualModifyTables, "Modify tables mismatch for SQL: " + testCase.sql);
        }
    }
}
