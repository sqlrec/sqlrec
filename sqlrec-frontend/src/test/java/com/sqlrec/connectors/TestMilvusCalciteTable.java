package com.sqlrec.connectors;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.FieldSchema;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.milvus.calcite.MilvusCalciteTable;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

@Tag("integration")
public class TestMilvusCalciteTable {
    @Test
    public void testMilvusTable() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tableMap = new HashMap<>();
                tableMap.put("t1", getMilvusTable());
                tableMap.put("t2", new MyTable());
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "insert into t1 (my_id, my_vector, my_varchar) values (1, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0], 'Alice1')",
                "insert into t1 (my_id, my_vector, my_varchar) values (2, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0], 'Alice2')",
                "insert into t1 (my_id, my_vector, my_varchar) values (3, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0], 'Alice3')",
                "select * from t1 where my_id = 1",
                "select * from t1 where my_id = 1 and my_varchar = 'Alice1'",
                "select * from t2 join t1 on t2.ID = t1.my_id",
                "delete from t1 where my_id = 3",
                "select * from t1 where my_varchar like 'Alice%'"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            } else {
                System.out.println("no result");
            }
        }
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

    public static Table getMilvusTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("my_id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("my_vector", "ARRAY<FLOAT>"));
        fieldSchemas.add(new FieldSchema("my_varchar", "VARCHAR"));

        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.url = "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":31530";
        milvusConfig.token = "root:Milvus";
        milvusConfig.database = "default";
        milvusConfig.collection = "test";
        milvusConfig.fieldSchemas = fieldSchemas;
        milvusConfig.primaryKey = "my_id";
        milvusConfig.primaryKeyIndex = 0;

        return new MilvusCalciteTable(milvusConfig);
    }
}