package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.connectors.milvus.calcite.MilvusCalciteTable;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.UdfManager;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Result contracts through the real Calcite -> Milvus -> projection path. */
@Tag("integration")
@Timeout(60)
class MilvusVectorLookupJoinIntegrationTest {
    private static final String COLLECTION = "sqlrec_vector_join_" + UUID.randomUUID().toString().replace("-", "");
    private static final List<Float> X = List.of(1f, 0f, 0f, 0f);
    private static final List<Float> Y = List.of(0f, 1f, 0f, 0f);
    private static final Map<Long, Candidate> CANDIDATES = Map.of(
            101L, new Candidate("Alpha", "book", X),
            102L, new Candidate("Beta", "book", List.of(.8f, .6f, 0f, 0f)),
            201L, new Candidate("Gamma", "movie", Y),
            202L, new Candidate("Delta", "movie", List.of(.6f, .8f, 0f, 0f)));
    private static MilvusClientV2 client;
    private static CalciteSchema schema;

    @BeforeAll
    static void createCollectionAndInputs() throws Exception {
        String url = "http://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30022";
        client = new MilvusClientV2(ConnectConfig.builder().uri(url).token("root:Milvus").dbName("default").build());
        var collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(false)
                .fieldSchemaList(List.of(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64)
                                .isPrimaryKey(true).autoID(false).build(),
                        CreateCollectionReq.FieldSchema.builder().name("title").dataType(DataType.VarChar)
                                .maxLength(64).build(),
                        CreateCollectionReq.FieldSchema.builder().name("genres").dataType(DataType.Array)
                                .elementType(DataType.VarChar).maxCapacity(4).maxLength(32).build(),
                        CreateCollectionReq.FieldSchema.builder().name("embedding").dataType(DataType.FloatVector)
                                .dimension(4).build()))
                .build();
        // Exact search and non-tied, unit-length vectors make ranking assertions deterministic.
        client.createCollection(CreateCollectionReq.builder().databaseName("default").collectionName(COLLECTION)
                .collectionSchema(collectionSchema).consistencyLevel(ConsistencyLevel.STRONG)
                .indexParams(List.of(IndexParam.builder().fieldName("embedding").indexName("embedding")
                        .indexType(IndexParam.IndexType.FLAT).metricType(IndexParam.MetricType.COSINE).build()))
                .build());

        MilvusConfig config = new MilvusConfig();
        config.url = url;
        config.token = "root:Milvus";
        config.database = "default";
        config.collection = COLLECTION;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.fieldSchemas = List.of(new FieldSchema("id", "BIGINT"), new FieldSchema("title", "VARCHAR"),
                new FieldSchema("genres", "ARRAY<VARCHAR>"), new FieldSchema("embedding", "ARRAY<FLOAT>"));
        Table candidates = new MilvusCalciteTable(config);
        schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
                return Map.of(
                        "candidates", candidates,
                        "queries", new QueryTable(new Object[]{1L, "book", X}, new Object[]{2L, "movie", Y}),
                        // The global best match belongs to the wrong group for both inputs.
                        "crossed_queries", new QueryTable(new Object[]{1L, "book", Y}, new Object[]{2L, "movie", X}),
                        "mixed_queries", new QueryTable(new Object[]{1L, "book", X},
                                new Object[]{3L, "missing", X}, new Object[]{2L, "movie", Y}),
                        "unmatched_queries", new QueryTable(new Object[]{3L, "missing", X}),
                        "empty_queries", new QueryTable());
            }
        });
        UdfManager.addFunction(schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false), "ip",
                "com.sqlrec.udf.scalar.IpFunction");
        UdfManager.addFunction(schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false), "array_contains",
                "com.sqlrec.udf.scalar.ArrayContainsFunction");
        for (var entry : CANDIDATES.entrySet()) {
            Candidate candidate = entry.getValue();
            String vector = candidate.embedding().stream().map(String::valueOf).collect(Collectors.joining(","));
            execute("INSERT INTO candidates (id, title, genres, embedding) VALUES (" + entry.getKey()
                    + ", '" + candidate.title() + "', ARRAY['" + candidate.genre() + "'], ARRAY[" + vector + "])");
        }
        // A strong read also verifies that all four fixture rows were inserted.
        assertEquals(List.of(101L, 102L, 201L, 202L), execute("SELECT id FROM candidates ORDER BY id")
                .stream().map(row -> ((Number) row[0]).longValue()).toList());
    }

    @AfterAll
    static void dropCollection() {
        if (client == null) return;
        try {
            if (client.hasCollection(HasCollectionReq.builder().collectionName(COLLECTION).build())) {
                client.dropCollection(DropCollectionReq.builder().databaseName("default").collectionName(COLLECTION).build());
            }
        } finally {
            client.close();
        }
    }

    @Test
    void topKIsAppliedPerLeftRowAndKeepsTheirCandidatesSeparate() throws Exception {
        List<Object[]> rows = lookup("queries", "", 2);
        assertMatches(rows, Map.of(1L, List.of(101L, 102L), 2L, List.of(201L, 202L)));
        assertScores(rows, Map.of(1L, List.of(1d, .8d), 2L, List.of(1d, .8d)));
    }

    @Test
    void correlatedFilterIsAppliedBeforeTopK() throws Exception {
        List<Object[]> rows = lookup("crossed_queries", "array_contains(c.genres, q.category)", 1);
        assertMatches(rows, Map.of(1L, List.of(102L), 2L, List.of(202L)));
        assertScores(rows, Map.of(1L, List.of(.6d), 2L, List.of(.6d)));
        for (Object[] row : rows) {
            assertEquals(((Number) row[0]).longValue() == 1 ? "book" : "movie",
                    CANDIDATES.get(((Number) row[1]).longValue()).genre());
        }
    }

    @Test
    void rightSideScalarFilterChangesTheSelectedCandidates() throws Exception {
        List<Object[]> rows = lookup("queries", "c.id >= 102", 1);
        assertMatches(rows, Map.of(1L, List.of(102L), 2L, List.of(201L)));
        assertScores(rows, Map.of(1L, List.of(.8d), 2L, List.of(1d)));
    }

    @Test
    void unmatchedMiddleInputDoesNotDropLaterMatches() throws Exception {
        List<Object[]> rows = lookup("mixed_queries", "array_contains(c.genres, q.category)", 1);
        assertMatches(rows, Map.of(1L, List.of(101L), 2L, List.of(201L)));
        assertScores(rows, Map.of(1L, List.of(1d), 2L, List.of(1d)));
    }

    @Test
    void emptyInputAndNoMatchingCandidatesProduceNoRows() throws Exception {
        assertTrue(lookup("empty_queries", "", 2).isEmpty());
        assertTrue(lookup("unmatched_queries", "array_contains(c.genres, q.category)", 2).isEmpty());
    }

    @Test
    void reorderedProjectionKeepsVectorsTitlesIdsAndScoresTogether() throws Exception {
        List<Object[]> rows = vectorQuery("SELECT q.id, c.embedding, c.id, c.title, "
                + "ip(q.embedding, c.embedding) AS score, q.embedding "
                + "FROM queries q JOIN candidates c ON 1=1 "
                + "ORDER BY ip(q.embedding, c.embedding) DESC LIMIT 2");
        List<Object[]> canonicalRows = new ArrayList<>();
        for (Object[] row : rows) {
            assertEquals(6, row.length);
            long queryId = ((Number) row[0]).longValue();
            Candidate candidate = CANDIDATES.get(((Number) row[2]).longValue());
            assertNotNull(candidate, "Unexpected candidate ID: " + row[2]);
            assertEquals(candidate.title(), row[3]);
            assertVectorEquals(candidate.embedding(), row[1]);
            assertVectorEquals(queryId == 1 ? X : Y, row[5]);
            canonicalRows.add(new Object[]{row[0], row[2], row[3], row[4]});
        }
        assertMatches(canonicalRows, Map.of(1L, List.of(101L, 102L), 2L, List.of(201L, 202L)));
        assertScores(canonicalRows, Map.of(1L, List.of(1d, .8d), 2L, List.of(1d, .8d)));
    }

    private static List<Object[]> lookup(String input, String filter, int topK) throws Exception {
        return vectorQuery("SELECT q.id, c.id, c.title, ip(q.embedding, c.embedding) AS score FROM "
                + input + " q JOIN candidates c ON 1=1 " + (filter.isEmpty() ? "" : "WHERE " + filter + " ")
                + "ORDER BY ip(q.embedding, c.embedding) DESC LIMIT " + topK);
    }

    private static List<Object[]> vectorQuery(String sql) throws Exception {
        var plan = new CompileManager().compileSql(CompileManager.parseSql(sql), schema, Consts.DEFAULT_SCHEMA_NAME, sql);
        assertTrue(plan.getPhysicalPlan().contains("SqlrecEnumerableVectorLookupJoin"), plan::getPhysicalPlan);
        ExecuteContextImpl context = new ExecuteContextImpl(schema);
        context.setVariable("IGNORE_JOIN_QUERY_EXCEPTION", "false");
        return plan.bind(schema, context).toList();
    }

    private static List<Object[]> execute(String sql) throws Exception {
        var plan = new CompileManager().compileSql(CompileManager.parseSql(sql), schema, Consts.DEFAULT_SCHEMA_NAME, sql);
        var rows = plan.bind(schema, new ExecuteContextImpl(schema));
        return rows == null ? List.of() : rows.toList();
    }

    private static void assertMatches(List<Object[]> rows, Map<Long, List<Long>> expected) {
        Map<Long, List<Long>> actual = new LinkedHashMap<>();
        for (Object[] row : rows) {
            assertEquals(4, row.length);
            long candidateId = ((Number) row[1]).longValue();
            assertTrue(CANDIDATES.containsKey(candidateId), "Unexpected candidate ID: " + candidateId);
            assertEquals(CANDIDATES.get(candidateId).title(), row[2]);
            actual.computeIfAbsent(((Number) row[0]).longValue(), ignored -> new ArrayList<>()).add(candidateId);
        }
        // Map equality ignores input traversal order but preserves rank and duplicate counts per query.
        assertEquals(expected, actual);
    }

    private static void assertScores(List<Object[]> rows, Map<Long, List<Double>> expected) {
        Map<Long, List<Double>> actual = new LinkedHashMap<>();
        for (Object[] row : rows) {
            double score = ((Number) row[3]).doubleValue();
            assertTrue(Double.isFinite(score), "Score must be finite");
            actual.computeIfAbsent(((Number) row[0]).longValue(), ignored -> new ArrayList<>()).add(score);
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (var entry : expected.entrySet()) {
            List<Double> scores = actual.get(entry.getKey());
            assertEquals(entry.getValue().size(), scores.size());
            for (int i = 0; i < scores.size(); i++) {
                assertEquals(entry.getValue().get(i), scores.get(i), 1e-5,
                        "Score mismatch for query " + entry.getKey() + " at rank " + i);
            }
        }
    }

    private static void assertVectorEquals(List<Float> expected, Object value) {
        List<?> actual = assertInstanceOf(List.class, value);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).doubleValue(), ((Number) actual.get(i)).doubleValue(), 1e-6);
        }
    }

    private record Candidate(String title, String genre, List<Float> embedding) {}

    private static final class QueryTable extends SqlRecTable implements ScannableTable {
        private final Object[][] rows;

        private QueryTable(Object[]... rows) { this.rows = rows; }

        @Override public Enumerable<Object[]> scan(DataContext root) { return Linq4j.asEnumerable(rows); }

        @Override public RelDataType getRowType(RelDataTypeFactory types) {
            return types.builder().add("id", SqlTypeName.BIGINT).add("category", SqlTypeName.VARCHAR)
                    .add("embedding", types.createArrayType(types.createSqlType(SqlTypeName.FLOAT), -1)).build();
        }
    }
}
