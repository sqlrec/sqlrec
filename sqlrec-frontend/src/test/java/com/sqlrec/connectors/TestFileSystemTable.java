package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.filesystem.calcite.FileSystemCalciteTable;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class TestFileSystemTable {

    @TempDir
    static Path tempDir;

    private static Path csvFile;
    private static Path jsonFile;

    @BeforeAll
    public static void initFiles() throws IOException {
        csvFile = tempDir.resolve("users.csv");
        Files.writeString(csvFile, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35");

        jsonFile = tempDir.resolve("products.json");
        Files.writeString(jsonFile, "[{\"id\": 1, \"name\": \"Book\", \"price\": 50}, {\"id\": 2, \"name\": \"Pen\", \"price\": 10}]");
    }

    @Test
    public void testCsvTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getCsvUsersTable());
        tableMap.put("t2", new MyTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // scan from CSV
        new SqlTestCase("select * from t1",
                Arrays.asList(new Object[]{1, "Alice", 30}, new Object[]{2, "Bob", 25}, new Object[]{3, "Charlie", 35})).test(schema);

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

        // insert new row (memory only)
        new SqlTestCase("insert into t1 (id, name, age) values (4, 'Dave', 40)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 4",
                Collections.singletonList(new Object[]{4, "Dave", 40})).test(schema);

        // delete
        new SqlTestCase("delete from t1 where id = 3", null).test(schema);
        new SqlTestCase("select * from t1 where id = 3",
                Collections.emptyList()).test(schema);

        // join with local table
        new SqlTestCase("select * from t2 join t1 on t2.id = t1.id",
                Arrays.asList(new Object[]{1, "Alice", 1, "Alice2", 31}, new Object[]{2, "Bob", 2, "Bob", 25})).test(schema);

        // left join
        new SqlTestCase("select * from t2 left join t1 on t2.id = t1.id",
                Arrays.asList(
                        new Object[]{1, "Alice", 1, "Alice2", 31},
                        new Object[]{2, "Bob", 2, "Bob", 25},
                        new Object[]{3, "Charlie", null, null, null}
                )).test(schema);

        // join with filter
        new SqlTestCase("select * from t2 join t1 on t2.id = t1.id where t1.name = 'Alice2'",
                Collections.singletonList(new Object[]{1, "Alice", 1, "Alice2", 31})).test(schema);

        // select with projection
        new SqlTestCase("select t1.id, t1.name from t2 join t1 on t2.id = t1.id",
                Arrays.asList(new Object[]{1, "Alice2"}, new Object[]{2, "Bob"})).test(schema);
    }

    @Test
    public void testJsonTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getJsonProductsTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // scan from JSON
        new SqlTestCase("select * from t1",
                Arrays.asList(new Object[]{1, "Book", 50}, new Object[]{2, "Pen", 10})).test(schema);

        // select with filter
        new SqlTestCase("select * from t1 where price > 20",
                Collections.singletonList(new Object[]{1, "Book", 50})).test(schema);

        // upsert
        new SqlTestCase("insert into t1 (id, name, price) values (1, 'Book2', 60)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1",
                Collections.singletonList(new Object[]{1, "Book2", 60})).test(schema);

        // insert new row
        new SqlTestCase("insert into t1 (id, name, price) values (3, 'Notebook', 30)", null).test(schema);
        new SqlTestCase("select * from t1 where id = 3",
                Collections.singletonList(new Object[]{3, "Notebook", 30})).test(schema);

        // delete
        new SqlTestCase("delete from t1 where id = 2", null).test(schema);
        new SqlTestCase("select * from t1 where id = 2",
                Collections.emptyList()).test(schema);
    }

    @Test
    public void testEmptyPathTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getEmptyPathTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // scan empty table
        new SqlTestCase("select * from t1",
                Collections.emptyList()).test(schema);

        // insert into empty table
        new SqlTestCase("insert into t1 (id, name, age) values (1, 'Alice', 30)", null).test(schema);
        new SqlTestCase("select * from t1",
                Collections.singletonList(new Object[]{1, "Alice", 30})).test(schema);
    }

    public static Table getCsvUsersTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("age", "INTEGER"));

        FileSystemConfig config = new FileSystemConfig();
        config.path = csvFile.toString();
        config.format = "csv";
        config.fieldSchemas = fieldSchemas;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;

        return new FileSystemCalciteTable(config);
    }

    public static Table getJsonProductsTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("price", "INTEGER"));

        FileSystemConfig config = new FileSystemConfig();
        config.path = jsonFile.toString();
        config.format = "json";
        config.fieldSchemas = fieldSchemas;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;

        return new FileSystemCalciteTable(config);
    }

    public static Table getEmptyPathTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("age", "INTEGER"));

        FileSystemConfig config = new FileSystemConfig();
        config.path = null;
        config.format = "csv";
        config.fieldSchemas = fieldSchemas;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;

        return new FileSystemCalciteTable(config);
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
