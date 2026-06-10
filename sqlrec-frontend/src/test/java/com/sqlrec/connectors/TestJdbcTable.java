package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.jdbc.calcite.JdbcCalciteTable;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;


public class TestJdbcTable {

    private static final String JDBC_URL = "jdbc:h2:mem:testjdbc;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    private static final String JDBC_USERNAME = "sa";
    private static final String JDBC_PASSWORD = "";

    @BeforeAll
    public static void initDatabase() throws Exception {
        Class.forName("org.h2.Driver");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "age INT)");
            stmt.execute("CREATE TABLE IF NOT EXISTS products (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "price INT)");
            stmt.execute("DELETE FROM users");
            stmt.execute("DELETE FROM products");
        }
    }

    @Test
    public void testJdbcTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getUsersTable());
        tableMap.put("t2", getProductsTable());
        tableMap.put("t3", new MyTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // insert (upsert semantics)
        new SqlTestCase("insert into t1 (id, name, age) values (1, 'Alice', 30)", null).test(schema);
        new SqlTestCase("insert into t1 (id, name, age) values (2, 'Bob', 25)", null).test(schema);
        new SqlTestCase("insert into t1 (id, name, age) values (3, 'Charlie', 35)", null).test(schema);
        new SqlTestCase("insert into t1 (id, name, age) values (3, 'Charlie', 35)", null).test(schema);

        // verify insert results
        new SqlTestCase("select * from t1 where id = 1",
                Collections.singletonList(new Object[]{1, "Alice", 30})).test(schema);
        new SqlTestCase("select * from t1 where id = 3",
                Collections.singletonList(new Object[]{3, "Charlie", 35})).test(schema);

        // select with filter
        new SqlTestCase("select * from t1 where age > 25",
                Arrays.asList(new Object[]{1, "Alice", 30}, new Object[]{3, "Charlie", 35})).test(schema);

        // select with multiple filters
        new SqlTestCase("select * from t1 where id = 2 and name = 'Bob'",
                Collections.singletonList(new Object[]{2, "Bob", 25})).test(schema);

        // upsert: insert with same primary key updates the row
        new SqlTestCase("insert into t1 (id, name, age) values (1, 'Alice2', 31)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1",
                Collections.singletonList(new Object[]{1, "Alice2", 31})).test(schema);

        // delete
        new SqlTestCase("delete from t1 where id = 3", null).test(schema);
        new SqlTestCase("select * from t1 where id = 3",
                Collections.emptyList()).test(schema);

        // insert into second table
        new SqlTestCase("insert into t2 (id, name, price) values (1, 'Book', 50)", null).test(schema);
        new SqlTestCase("insert into t2 (id, name, price) values (2, 'Pen', 10)", null).test(schema);

        // join with local table
        new SqlTestCase("select * from t3 join t1 on t3.id = t1.id",
                Arrays.asList(new Object[]{1, "Alice", 1, "Alice2", 31}, new Object[]{2, "Bob", 2, "Bob", 25})).test(schema);

        // left join
        new SqlTestCase("select * from t3 left join t1 on t3.id = t1.id",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice2", 31},
                        new Object[]{2, "Bob", 2, "Bob", 25},
                        new Object[]{3, "Charlie", null, null, null}
                )).test(schema);

        // join with filter
        new SqlTestCase("select * from t3 join t1 on t3.id = t1.id where t1.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice", 1, "Alice2", 31})).test(schema);

        // select with projection
        new SqlTestCase("select t1.id, t1.name from t3 join t1 on t3.id = t1.id",
                Arrays.asList(new Object[]{1, "Alice2"}, new Object[]{2, "Bob"})).test(schema);
    }

    public static Table getUsersTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("age", "INTEGER"));

        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.url = JDBC_URL;
        jdbcConfig.username = JDBC_USERNAME;
        jdbcConfig.password = JDBC_PASSWORD;
        jdbcConfig.driver = "org.h2.Driver";
        jdbcConfig.tableName = "users";
        jdbcConfig.fieldSchemas = fieldSchemas;
        jdbcConfig.primaryKey = "id";
        jdbcConfig.primaryKeyIndex = 0;
        jdbcConfig.maxCacheSize = 100000;
        jdbcConfig.cacheTtl = 0;

        return new JdbcCalciteTable(jdbcConfig);
    }

    public static Table getProductsTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("price", "INTEGER"));

        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.url = JDBC_URL;
        jdbcConfig.username = JDBC_USERNAME;
        jdbcConfig.password = JDBC_PASSWORD;
        jdbcConfig.driver = "org.h2.Driver";
        jdbcConfig.tableName = "products";
        jdbcConfig.fieldSchemas = fieldSchemas;
        jdbcConfig.primaryKey = "id";
        jdbcConfig.primaryKeyIndex = 0;
        jdbcConfig.maxCacheSize = 100000;
        jdbcConfig.cacheTtl = 0;

        return new JdbcCalciteTable(jdbcConfig);
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }
    }
}
