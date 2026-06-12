package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.mongodb.calcite.MongoCalciteTable;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag("integration")
public class TestMongoTable {

    private static final String MONGO_URI = "mongodb://sqlrec:abc123456@" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30281";

    @Test
    public void testMongoTable() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getMongoTable());
        tableMap.put("t2", new MyTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        CalciteSchemaFactory.setGlobalSchema(schema);

        // insert
        new SqlTestCase("insert into t1 (id, name, age) values (1, 'Alice', 30)", null).test(schema);
        new SqlTestCase("insert into t1 (id, name, age) values (2, 'Bob', 25)", null).test(schema);
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

        // cleanup
        new SqlTestCase("delete from t1 where id = 1", null).test(schema);
        new SqlTestCase("delete from t1 where id = 2", null).test(schema);
    }

    public static Table getMongoTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("age", "INTEGER"));

        MongoConfig mongoConfig = new MongoConfig();
        mongoConfig.uri = MONGO_URI;
        mongoConfig.database = "sqlrec_test";
        mongoConfig.collection = "test_mongo_table";
        mongoConfig.fieldSchemas = fieldSchemas;
        mongoConfig.primaryKey = "id";
        mongoConfig.primaryKeyIndex = 0;
        mongoConfig.maxCacheSize = 100000;
        mongoConfig.cacheTtl = 30;

        return new MongoCalciteTable(mongoConfig);
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
