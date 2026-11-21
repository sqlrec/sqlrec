package com.sqlrec.compiler;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlTypeCheckerTest {

    private static class SqlTestCase {
        String sql;
        List<String> expectedTables;

        SqlTestCase(String sql, List<String> expectedTables) {
            this.sql = sql;
            this.expectedTables = expectedTables;
        }
    }

    @Test
    public void testSqlTypeChecker() throws Exception {
        List<SqlTestCase> testCases = List.of(
                new SqlTestCase("use `default`", List.of()),
                new SqlTestCase("select * from db1.t1", List.of("db1.t1")),
                new SqlTestCase("select * from ( select * from db1.t1) t", List.of("db1.t1")),
                new SqlTestCase("SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME", List.of("mytable")),
                new SqlTestCase("select * from t1 union select * from t2 union select * from t3", List.of("t1", "t2", "t3")),
                new SqlTestCase("select * from t1 join t2 on t1.id = t2.id", List.of("t1", "t2")),
                new SqlTestCase("update t1 SET column1 = value1 where id = 1", List.of("t1")),
                new SqlTestCase("update t1 SET column1 = value1 where id in ( select id from t2)", List.of("t1", "t2")),
                new SqlTestCase("delete from t1 where id = 1", List.of("t1")),
                new SqlTestCase("delete from t1 where id in ( select id from t2)", List.of("t1", "t2")),
                new SqlTestCase("insert into t1 (id, column1) values (1, 'value1')", List.of("t1")),
                new SqlTestCase("insert into t1 (id, column1) select id, column1 from t2", List.of("t1", "t2")),
                new SqlTestCase("insert into db1.t1 (id, column1) select id, column1 from db1.t2", List.of("db1.t1", "db1.t2"))
        );

        for (SqlTestCase testCase : testCases) {
            SqlNode flinkSqlNode = CompileManager.parseSql(testCase.sql);

            List<String> actualTables = NodeUtils.getTableFromSqlNode(flinkSqlNode);
            assertEquals(testCase.expectedTables, actualTables, "Tables mismatch for SQL: " + testCase.sql);
        }
    }

    @Test
    public void topLevelCteIsNotLocallyCompilable() throws Exception {
        SqlNode sqlNode = CompileManager.parseSql(
                "with cte as (select 1 as id) select * from cte"
        );

        assertFalse(SqlTypeChecker.isFlinkSqlCompilable(
                sqlNode,
                createSchema(),
                Consts.DEFAULT_SCHEMA_NAME
        ));
    }

    @Test
    public void cacheCteCannotBypassLocalCheckByUsingARealTableName() throws Exception {
        CalciteSchema schema = createSchema("physical_table");
        SqlNode sqlNode = CompileManager.parseSql(
                "cache table result_table as "
                        + "with physical_table as (select 1 as id) "
                        + "select * from physical_table"
        );

        assertFalse(SqlTypeChecker.isFlinkSqlCompilable(
                sqlNode,
                schema,
                Consts.DEFAULT_SCHEMA_NAME
        ));
    }

    @Test
    public void insertCteIsNotLocallyCompilable() throws Exception {
        CalciteSchema schema = createSchema("sink_table", "physical_table");
        SqlNode sqlNode = CompileManager.parseSql(
                "insert into sink_table "
                        + "with physical_table as (select 1 as id) "
                        + "select * from physical_table"
        );

        assertFalse(SqlTypeChecker.isFlinkSqlCompilable(
                sqlNode,
                schema,
                Consts.DEFAULT_SCHEMA_NAME
        ));
    }

    @Test
    public void ordinarySelectRemainsLocallyCompilable() throws Exception {
        CalciteSchema schema = createSchema("physical_table");
        SqlNode sqlNode = CompileManager.parseSql("select * from physical_table");

        assertTrue(SqlTypeChecker.isFlinkSqlCompilable(
                sqlNode,
                schema,
                Consts.DEFAULT_SCHEMA_NAME
        ));
    }

    private static CalciteSchema createSchema(String... tableNames) {
        Map<String, Table> tables = new LinkedHashMap<>();
        for (String tableName : tableNames) {
            tables.put(tableName, new CacheTable(tableName, null, List.of()));
        }

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tables;
            }
        });
        return schema;
    }
}
