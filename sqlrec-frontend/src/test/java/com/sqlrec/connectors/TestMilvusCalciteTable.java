package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.connectors.milvus.calcite.MilvusCalciteTable;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptPlanner;
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
    private static final long TEST_ID_BASE = 9_000_000_000L;
    private static final long VISIBILITY_TIMEOUT_MILLIS = 15_000L;
    private static final long VISIBILITY_POLL_INTERVAL_MILLIS = 100L;

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
        CalciteSchemaFactory.setGlobalSchema(schema);

        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "ip",
                "com.sqlrec.udf.scalar.IpFunction"
        );
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "array_contains",
                "com.sqlrec.udf.scalar.ArrayContainsFunction"
        );
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "array_contains_all",
                "com.sqlrec.udf.scalar.ArrayContainsAllFunction"
        );
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "array_contains_any",
                "com.sqlrec.udf.scalar.ArrayContainsAnyFunction"
        );

        cleanupTestRows(schema);
        try {
            new SqlTestCase(insertSql(TEST_ID_BASE, "SqlRec Coverage Movie Alpha", "Action", "Drama"), null).test(schema);
            new SqlTestCase(insertSql(TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta", "Comedy"), null).test(schema);
            new SqlTestCase(insertSql(TEST_ID_BASE + 2, "SqlRec Coverage Film Gamma", "Thriller", "Action"), null).test(schema);
            new SqlTestCase(insertSql(TEST_ID_BASE + 3, "SqlRec Coverage Movie Deleted", "Sci-Fi"), null).test(schema);
            new SqlTestCase(insertSql(TEST_ID_BASE + 4, "SqlRec Coverage \"Quoted\"", "Drama"), null).test(schema);

            awaitRowsVisible(
                    schema,
                    "select id, title from t1 where id between " + TEST_ID_BASE + " and "
                            + (TEST_ID_BASE + 4) + " order by id",
                    rows(
                            row(TEST_ID_BASE, "SqlRec Coverage Movie Alpha"),
                            row(TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta"),
                            row(TEST_ID_BASE + 2, "SqlRec Coverage Film Gamma"),
                            row(TEST_ID_BASE + 3, "SqlRec Coverage Movie Deleted"),
                            row(TEST_ID_BASE + 4, "SqlRec Coverage \"Quoted\"")));

            new SqlTestCase("select id, title from t1 where id = " + (TEST_ID_BASE + 1),
                    rows(row(TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta"))).test(schema);
            new SqlTestCase("select id, title from t1 where id = " + (TEST_ID_BASE + 1)
                    + " and title = 'SqlRec Coverage Movie Beta'",
                    rows(row(TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta"))).test(schema);
            new SqlTestCase("select id, title from t1 where id > " + TEST_ID_BASE
                    + " and id <= " + (TEST_ID_BASE + 2) + " order by id",
                    rows(
                            row(TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta"),
                            row(TEST_ID_BASE + 2, "SqlRec Coverage Film Gamma"))).test(schema);
            new SqlTestCase("select id from t1 where id >= " + (TEST_ID_BASE + 1)
                    + " and id < " + (TEST_ID_BASE + 3) + " order by id",
                    rows(row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where (id = " + TEST_ID_BASE
                    + " or id = " + (TEST_ID_BASE + 1) + ") and not (id = "
                    + (TEST_ID_BASE + 3) + ") order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 1))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4) + " and id <> " + (TEST_ID_BASE + 1) + " order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 2),
                            row(TEST_ID_BASE + 3), row(TEST_ID_BASE + 4))).test(schema);
            new SqlTestCase("select id from t1 where id in (" + TEST_ID_BASE + ", "
                    + (TEST_ID_BASE + 2) + ") order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4) + " and id not in (" + TEST_ID_BASE + ", "
                    + (TEST_ID_BASE + 2) + ") order by id",
                    rows(row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 3),
                            row(TEST_ID_BASE + 4))).test(schema);
            new SqlTestCase("select id from t1 where id between " + (TEST_ID_BASE + 1)
                    + " and " + (TEST_ID_BASE + 2) + " order by id",
                    rows(row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where id + 1 > " + (TEST_ID_BASE + 1)
                    + " and id <= " + (TEST_ID_BASE + 2) + " order by id",
                    rows(row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where title like 'SqlRec Coverage Movie %' order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 3))).test(schema);
            new SqlTestCase("select id from t1 where title is null and id between "
                    + TEST_ID_BASE + " and " + (TEST_ID_BASE + 4), Collections.emptyList()).test(schema);
            new SqlTestCase("select id from t1 where title is not null and id between "
                    + TEST_ID_BASE + " and " + (TEST_ID_BASE + 4) + " order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 2),
                            row(TEST_ID_BASE + 3), row(TEST_ID_BASE + 4))).test(schema);
            new SqlTestCase("select id from t1 where title = 'SqlRec Coverage \"Quoted\"'",
                    rows(row(TEST_ID_BASE + 4))).test(schema);
            // Exercises the non-empty match-all predicate used for scans without filters.
            new SqlTestCase("select id from t1 limit 1", null).test(schema);

            new SqlTestCase("select t2.ID, t1.id, t1.title from t2 join t1 on t2.ID = t1.id order by t2.ID",
                    rows(
                            row(TEST_ID_BASE, TEST_ID_BASE, "SqlRec Coverage Movie Alpha"),
                            row(TEST_ID_BASE + 1, TEST_ID_BASE + 1, "SqlRec Coverage Movie Beta"),
                            row(TEST_ID_BASE + 2, TEST_ID_BASE + 2, "SqlRec Coverage Film Gamma"))).test(schema);
            new SqlTestCase("update t1 set title = 'SqlRec Coverage Movie Beta Updated' where id = "
                    + (TEST_ID_BASE + 1), null).test(schema);
            // Milvus queries use eventual consistency by default. Verify that the upsert
            // keeps the row addressable, but do not make this integration test depend on
            // when the updated scalar value becomes visible.
            new SqlTestCase("select id from t1 where id = " + (TEST_ID_BASE + 1),
                    rows(row(TEST_ID_BASE + 1))).test(schema);
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
                            EnumerableCalc(expr#0..8=[{inputs}], proj#0..8=[{exprs}])
                              SqlrecEnumerableVectorLookupJoin(leftEmbeddingIndex=[3], rightEmbeddingField=[embedding], topKPerLeftRow=[10])
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
                            EnumerableCalc(expr#0..8=[{inputs}], id=[$t4], title=[$t5], genres=[$t6], embedding=[$t7], EXPR$4=[$t8])
                              SqlrecEnumerableVectorLookupJoin(leftEmbeddingIndex=[3], rightEmbeddingField=[embedding], topKPerLeftRow=[10])
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
                            EnumerableCalc(expr#0..8=[{inputs}], ID=[$t0], id0=[$t4], title=[$t5], EXPR$3=[$t8])
                              SqlrecEnumerableVectorLookupJoin(leftEmbeddingIndex=[3], rightEmbeddingField=[embedding])
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
                            EnumerableCalc(expr#0..8=[{inputs}], ID=[$t0], id0=[$t4], title=[$t5], EXPR$3=[$t8])
                              SqlrecEnumerableVectorLookupJoin(leftEmbeddingIndex=[3], rightEmbeddingField=[embedding], topKPerLeftRow=[10])
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
                            EnumerableCalc(expr#0..8=[{inputs}], proj#0..8=[{exprs}])
                              SqlrecEnumerableVectorLookupJoin(pushedFilter=[=($1, $5)], leftEmbeddingIndex=[3], rightEmbeddingField=[embedding], topKPerLeftRow=[10])
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
                            EnumerableCalc(expr#0..8=[{inputs}], proj#0..8=[{exprs}])
                              SqlrecEnumerableVectorLookupJoin(pushedFilter=[>=($4, 1)], leftEmbeddingIndex=[3], rightEmbeddingField=[embedding], topKPerLeftRow=[10])
                                EnumerableTableScan(table=[[default, t2]])
                                EnumerableTableScan(table=[[default, t1]])""",
                    null).test(schema);
            // An unsupported right-side predicate must prevent the vector lookup rule
            // from firing, so the original Calcite plan keeps the complete condition.
            new SqlTestCase("select * from t2 join t1 on 1=1 " +
                    "where t2.title = t1.title and t1.id + 1 > 1 " +
                    "order by ip(t2.embedding, t1.embedding) limit 10", null,
                    new RelOptPlanner.CannotPlanException("Unsupported vector filter")).test(schema);
            new SqlTestCase("select * from t2 join t1 on 1=1 " +
                    "where t2.ID > " + TEST_ID_BASE
                    + " and t1.title = 'SqlRec Coverage Movie Beta Updated' " +
                    "order by ip(t2.embedding, t1.embedding) limit 10", null).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4) + " and array_contains(genres, 'Action') order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4) + " and array_contains(genres, 'Comedy') order by id",
                    rows(row(TEST_ID_BASE + 1))).test(schema);
            new SqlTestCase("select id from t1 where id = " + (TEST_ID_BASE + 1)
                    + " and array_contains(genres, 'Comedy')",
                    rows(row(TEST_ID_BASE + 1))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4)
                    + " and array_contains_all(genres, ARRAY['Action', 'Drama']) order by id",
                    rows(row(TEST_ID_BASE))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4)
                    + " and array_contains_any(genres, ARRAY['Action', 'Comedy']) order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 1), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4)
                    + " and array_contains_all(genres, ARRAY['Action']) order by id",
                    rows(row(TEST_ID_BASE), row(TEST_ID_BASE + 2))).test(schema);
            new SqlTestCase("select id from t1 where id between " + TEST_ID_BASE + " and "
                    + (TEST_ID_BASE + 4)
                    + " and array_contains_any(genres, ARRAY['Sci-Fi', 'Thriller']) order by id",
                    rows(row(TEST_ID_BASE + 2), row(TEST_ID_BASE + 3))).test(schema);
            new SqlTestCase("select * from t2 join t1 on 1=1 where array_contains(t1.genres, t2.title) order by ip(t2.embedding, t1.embedding) limit 1", null).test(schema);
            new SqlTestCase("select * from t2 join t1 on 1=1 where array_contains_all(t1.genres, t2.genres) order by ip(t2.embedding, t1.embedding) limit 1", null).test(schema);
            new SqlTestCase("select * from t2 join t1 on 1=1 where array_contains_any(t1.genres, t2.genres) order by ip(t2.embedding, t1.embedding) limit 1", null).test(schema);
            // Execute DELETE after result assertions because Milvus query visibility is
            // eventually consistent; finally performs idempotent cleanup as well.
            new SqlTestCase("delete from t1 where id = " + (TEST_ID_BASE + 3), null).test(schema);
        } finally {
            cleanupTestRows(schema);
        }
    }

    private static Object[] row(Object... values) {
        return values;
    }

    private static List<Object[]> rows(Object[]... values) {
        return Arrays.asList(values);
    }

    private static String insertSql(long id, String title, String... genres) {
        StringJoiner genreValues = new StringJoiner(", ");
        for (String genre : genres) {
            genreValues.add(sqlString(genre));
        }
        return "insert into t1 (id, embedding, title, genres) values ("
                + id + ", " + embeddingSql() + ", " + sqlString(title)
                + ", ARRAY[" + genreValues + "])";
    }

    private static String embeddingSql() {
        StringJoiner values = new StringJoiner(", ", "ARRAY[", "]");
        for (int i = 1; i <= 64; i++) {
            values.add(i + ".0");
        }
        return values.toString();
    }

    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static void awaitRowsVisible(
            CalciteSchema schema, String sql, List<Object[]> expectedRows) throws Exception {
        long deadline = System.nanoTime() + VISIBILITY_TIMEOUT_MILLIS * 1_000_000L;
        AssertionError lastFailure = null;
        do {
            try {
                new SqlTestCase(sql, expectedRows)
                        .setDebugOutput(false)
                        .test(schema);
                return;
            } catch (AssertionError failure) {
                lastFailure = failure;
            }
            Thread.sleep(VISIBILITY_POLL_INTERVAL_MILLIS);
        } while (System.nanoTime() < deadline);

        throw new AssertionError(
                "Milvus test rows were not visible within " + VISIBILITY_TIMEOUT_MILLIS + " ms",
                lastFailure);
    }

    private static void cleanupTestRows(CalciteSchema schema) throws Exception {
        for (int i = 0; i < 5; i++) {
            new SqlTestCase("delete from t1 where id = " + (TEST_ID_BASE + i), null)
                    .setDebugOutput(false)
                    .test(schema);
        }
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
                    {TEST_ID_BASE, "Action", Arrays.asList("Action", "Drama"), embedding1},
                    {TEST_ID_BASE + 1, "Comedy", Arrays.asList("Comedy"), embedding2},
                    {TEST_ID_BASE + 2, "Sci-Fi", Arrays.asList("Sci-Fi", "Thriller"), embedding3}
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
        milvusConfig.url = "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30022";
        milvusConfig.token = "root:Milvus";
        milvusConfig.database = "default";
        milvusConfig.collection = "item_embedding";
        milvusConfig.fieldSchemas = fieldSchemas;
        milvusConfig.primaryKey = "id";
        milvusConfig.primaryKeyIndex = 0;

        return new MilvusCalciteTable(milvusConfig);
    }
}
