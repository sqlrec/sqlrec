package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.milvus.calcite.MilvusCalciteTable;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.udf.UdfManager;
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

        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "ip",
                "com.sqlrec.udf.scalar.IpFunction"
        );

        new SqlTestCase("insert into t1 (id, embedding, title, genres) values (0, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0], 'Movie Title 0', ARRAY['Action', 'Drama'])", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, title, genres) values (1, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0], 'Movie Title 1', ARRAY['Comedy'])", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, title, genres) values (2, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0], 'Movie Title 2', ARRAY['Thriller', 'Action'])", null).test(schema);
        new SqlTestCase("insert into t1 (id, embedding, title, genres) values (3, ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0], 'Movie Title 3', ARRAY['Sci-Fi'])", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1", null).test(schema);
        new SqlTestCase("select * from t1 where id = 1 and title = 'Movie Title 1'", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on t2.ID = t1.id", null).test(schema);
        new SqlTestCase("delete from t1 where id = 3", null).test(schema);
        new SqlTestCase("select * from t1 where title like 'Movie%'", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null).test(schema);
        new SqlTestCase("cache table tmp as select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null).test(schema);
        new SqlTestCase("select * from tmp", null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$8], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], title=[$1], genres=[$2], embedding=[$3], id0=[$4], title0=[$5], genres0=[$6], embedding0=[$7], EXPR$8=[ip($3, $7)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6, 7, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t1.* from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$4], dir0=[ASC], fetch=[10])
                          LogicalProject(id=[$4], title=[$5], genres=[$6], embedding=[$7], EXPR$4=[ip($3, $7)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], limit=[10], projects=[[4, 5, 6, 7, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t2.ID, t1.id, t1.title from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding)", null,
                """
                        LogicalSort(sort0=[$3], dir0=[ASC])
                          LogicalProject(ID=[$0], id0=[$4], title=[$5], EXPR$3=[ip($3, $7)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], projects=[[0, 4, 5, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select t2.ID, t1.id, t1.title from t2 join t1 on 1=1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$3], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], id0=[$4], title=[$5], EXPR$3=[ip($3, $7)])
                            LogicalJoin(condition=[true], joinType=[inner])
                              LogicalTableScan(table=[[default, t2]])
                              LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 4, 5, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 where t2.title = t1.title order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$8], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], title=[$1], genres=[$2], embedding=[$3], id0=[$4], title0=[$5], genres0=[$6], embedding0=[$7], EXPR$8=[ip($3, $7)])
                            LogicalFilter(condition=[=($1, $5)])
                              LogicalJoin(condition=[true], joinType=[inner])
                                LogicalTableScan(table=[[default, t2]])
                                LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], filterCondition=[=($1, $5)], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6, 7, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
        new SqlTestCase("select * from t2 join t1 on 1=1 where t1.id >= 1 order by ip(t2.embedding, t1.embedding) limit 10", null,
                """
                        LogicalSort(sort0=[$8], dir0=[ASC], fetch=[10])
                          LogicalProject(ID=[$0], title=[$1], genres=[$2], embedding=[$3], id0=[$4], title0=[$5], genres0=[$6], embedding0=[$7], EXPR$8=[ip($3, $7)])
                            LogicalFilter(condition=[>=($4, 1)])
                              LogicalJoin(condition=[true], joinType=[inner])
                                LogicalTableScan(table=[[default, t2]])
                                LogicalTableScan(table=[[default, t1]])""",
                """
                        SqlrecEnumerableVectorJoin(condition=[true], joinType=[INNER], filterCondition=[>=($4, 1)], leftEmbeddingColIndex=[3], rightEmbeddingColName=[embedding], limit=[10], projects=[[0, 1, 2, 3, 4, 5, 6, 7, 8]])
                          EnumerableTableScan(table=[[default, t2]])
                          EnumerableTableScan(table=[[default, t1]])""",
                null).test(schema);
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            List<Float> embedding1 = new ArrayList<>();
            List<Float> embedding2 = new ArrayList<>();
            List<Float> embedding3 = new ArrayList<>();
            for (int i = 1; i <= 64; i++) {
                embedding1.add((float) i);
                embedding2.add((float) i);
                embedding3.add((float) i);
            }
            return Linq4j.asEnumerable(new Object[][]{
                    {1L, "Action Movie", Arrays.asList("Action", "Drama"), embedding1},
                    {2L, "Comedy Movie", Arrays.asList("Comedy"), embedding2},
                    {3L, "Sci-Fi Movie", Arrays.asList("Sci-Fi", "Thriller"), embedding3}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.BIGINT)
                    .add("title", SqlTypeName.VARCHAR)
                    .add("genres", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
                    .add("embedding", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.FLOAT), -1))
                    .build();
        }
    }

    public static Table getMilvusTable() {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("id", "BIGINT"));
        fieldSchemas.add(new FieldSchema("title", "VARCHAR"));
        fieldSchemas.add(new FieldSchema("genres", "ARRAY<VARCHAR>"));
        fieldSchemas.add(new FieldSchema("embedding", "ARRAY<FLOAT>"));

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