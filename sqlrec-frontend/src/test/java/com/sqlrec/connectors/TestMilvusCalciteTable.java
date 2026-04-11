package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.milvus.calcite.MilvusCalciteTable;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.SchemaUtils;
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

import java.util.*;

@Tag("integration")
public class TestMilvusCalciteTable {
    @Test
    public void testMilvusTable() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tableMap = new HashMap<>();
                tableMap.put("t1", getMilvusTable());
                tableMap.put("t2", new MyTable());
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        SchemaUtils.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "ip",
                "com.sqlrec.udf.scalar.IpFunction"
        );

        new SqlTestCase("insert into t1 (id, embedding, name) values (0, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'Alice')", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, name) values (1, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'Alice1')", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, name) values (2, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'Alice2')", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, name) values (3, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'Alice3')", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and name = 'Alice1'", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on t2.ID = t1.id", null).test(schema);
        new SqlTestCase("delete from t1 where id = 3", null).test(schema);
        new SqlTestCase("select * from t1 where name like 'Alice%'", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null).test(schema);
        new SqlTestCase("cache table tmp as select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null).test(schema);
        new SqlTestCase("select * from tmp", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$6], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], NAME=[$1], embedding=[$2], id0=[$3], embedding0=[$4], name0=[$5], EXPR$6=[ip($2, $4)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t1.* from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$3], dir0=[ASC], fetch=[10])
                          LogicalProject(id=[$3], embedding=[$4], name=[$5], EXPR$3=[ip($2, $4)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], limit=[10], projects=[[3, 4, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t2.ID, t1.id, t1.name from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null,
                """
                        LogicalSort(sort0=[$3], dir0=[ASC])
                          LogicalProject(ID=[$0], id0=[$3], name=[$5], EXPR$3=[ip($2, $4)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], projects=[[0, 3, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t2.ID, t1.id, t1.name from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$3], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], id0=[$3], name=[$5], EXPR$3=[ip($2, $4)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 3, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 where t2.name = t1.name order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$6], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], NAME=[$1], embedding=[$2], id0=[$3], embedding0=[$4], name0=[$5], EXPR$6=[ip($2, $4)])
                            LogicalFilter(condition=[=($1, $5)])
                              LogicalJoin(condition=[true], joinType=[inner])
                                LogicalTableScan(table=[[default, t2]])
                                LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], filterCondition=[=($1, $5)], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 where t1.id >= 1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$6], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], NAME=[$1], embedding=[$2], id0=[$3], embedding0=[$4], name0=[$5], EXPR$6=[ip($2, $4)])
                            LogicalFilter(condition=[>=($3, 1)])
                              LogicalJoin(condition=[true], joinType=[inner])
                                LogicalTableScan(table=[[default, t2]])
                                LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], filterCondition=[>=($3, 1)], leftEmbeddingColIndex=[2], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice", Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f)},
                    {2, "Bob", Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f)},
                    {3, "Charlie", Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f)}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR)
                    .add("embedding", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), -1))
                    .build();
        }
    }

    public static Table getMilvusTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "INTEGER"));
        fieldSchemas.add(new FieldSchema("embedding", "ARRAY<FLOAT>"));
        fieldSchemas.add(new FieldSchema("name", "VARCHAR"));

        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.url = "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":31530";
        milvusConfig.token = "root:Milvus";
        milvusConfig.database = "default";
        milvusConfig.collection = "item_embedding";
        milvusConfig.fieldSchemas = fieldSchemas;
        milvusConfig.primaryKey = "id";
        milvusConfig.primaryKeyIndex = 0;

        return new MilvusCalciteTable(milvusConfig);
    }
}