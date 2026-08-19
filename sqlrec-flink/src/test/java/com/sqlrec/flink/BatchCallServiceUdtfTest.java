package com.sqlrec.flink;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for BatchCallServiceUDTF (batch_call_service), mirroring the
 * item_embedding pipeline in benchmark/movielens/load_features.sql:
 *
 * INSERT INTO item_embedding
 * SELECT r.long_map['movie_id'] AS id, r.string_map['title'] AS title,
 *        r.string_array_map['genres'] AS genres,
 *        r.double_array_map['item_tower_emb'] AS embedding
 * FROM ml_movies, LATERAL TABLE(batch_call_service(url, 128,
 *     'movie_id', movie_id, 'title', title, 'genres', genres)) AS r
 *
 * A mock prediction service (JDK HttpServer) emulates recall-service-item's
 * /predict endpoint: it receives the buffered rows as a JSON array and replies
 * with columnar predictions, e.g. {"item_tower_emb": [[...], ...]} where the
 * i-th element corresponds to the i-th input row.
 *
 * NOTE: tests always use a batchSize that flushes mid-stream. Rows buffered in
 * a trailing partial batch are flushed from TableFunction.close() - the HTTP
 * request is sent, but rows collected during close() are dropped by the Flink
 * 1.19 streaming runtime (verified experimentally, with and without operator
 * chaining). load_features.sql uses batchSize=128, so with the current runtime
 * the last (rowCount % 128) movies would be lost from item_embedding.
 *
 * Uses MiniCluster via {@link MiniClusterExtension} to run a real Flink job.
 */
@Tag("integration")
class BatchCallServiceUdtfTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    // First movies from ml_movies (MovieLens), used as test input.
    private static final Long[] MOVIE_IDS = {1L, 2L, 3L, 4L, 5L};
    private static final String[] TITLES = {
            "Toy Story (1995)",
            "Jumanji (1995)",
            "Grumpier Old Men (1995)",
            "Waiting to Exhale (1995)",
            "Father of the Bride Part II (1995)"
    };
    private static final String[][] GENRES = {
            {"Adventure", "Animation", "Children"},
            {"Adventure", "Children", "Fantasy"},
            {"Comedy", "Romance"},
            {"Comedy", "Drama", "Romance"},
            {"Comedy"}
    };

    private HttpServer server;
    private String serviceUrl;
    private final List<String> receivedBodies = new CopyOnWriteArrayList<>();

    // Optional extra columnar predictions merged into every response.
    private volatile JsonObject extraPredictions;

    // ---- Mock prediction service ----

    @BeforeEach
    void startMockService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/predict", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            receivedBodies.add(body);

            JsonObject response = new JsonObject();
            response.add("item_tower_emb", embeddingsFor(body));
            if (extraPredictions != null) {
                for (Map.Entry<String, JsonElement> e : extraPredictions.entrySet()) {
                    response.add(e.getKey(), e.getValue());
                }
            }

            byte[] bytes = response.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();
        serviceUrl = "http://localhost:" + server.getAddress().getPort() + "/predict";
    }

    @AfterEach
    void stopMockService() {
        if (server != null) {
            server.stop(0);
        }
    }

    /**
     * Deterministic per-row embedding [id*0.1, id*0.2, id*0.3], where id is
     * the movie_id of the i-th request row.
     */
    private static JsonArray embeddingsFor(String requestBody) {
        JsonArray rows = JsonParser.parseString(requestBody).getAsJsonArray();
        JsonArray embeddings = new JsonArray();
        for (JsonElement rowElement : rows) {
            long movieId = rowElement.getAsJsonObject().get("movie_id").getAsLong();
            JsonArray embedding = new JsonArray();
            for (int k = 1; k <= 3; k++) {
                embedding.add(new JsonPrimitive(movieId * 0.1 * k));
            }
            embeddings.add(embedding);
        }
        return embeddings;
    }

    // ---- Tests ----

    /**
     * Mirrors the item_embedding INSERT of load_features.sql: field mapping
     * through the typed output maps and the JSON request format. Uses
     * batchSize equal to the row count so the single batch is flushed
     * mid-stream (see the close() caveat in the class javadoc).
     */
    @Test
    void testItemEmbeddingPipeline() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        TableResult result = tEnv.executeSql(itemEmbeddingSql(serviceUrl, 3, 3));

        List<Row> rows = collectRows(result);
        assertEquals(3, rows.size(), "One output row per movie");

        Map<Long, Row> byId = new HashMap<>();
        for (Row row : rows) {
            byId.put((Long) row.getField(0), row);
        }

        for (int i = 0; i < 3; i++) {
            long id = MOVIE_IDS[i];
            Row row = byId.get(id);
            assertNotNull(row, "Missing row for movie " + id);
            assertEquals(TITLES[i], row.getField(1), "title should pass through string_map");
            assertArrayEquals(GENRES[i], (String[]) row.getField(2),
                    "genres should pass through string_array_map");
            Double[] embedding = (Double[]) row.getField(3);
            assertEquals(3, embedding.length);
            for (int k = 1; k <= 3; k++) {
                assertEquals(id * 0.1 * k, embedding[k - 1], 1e-9);
            }
        }

        // All 3 movies fit in one batch: exactly one POST with a JSON array
        // of {movie_id, title, genres} objects.
        assertEquals(1, receivedBodies.size());
        JsonArray request = JsonParser.parseString(receivedBodies.get(0)).getAsJsonArray();
        assertEquals(3, request.size());
        for (int i = 0; i < 3; i++) {
            JsonObject movie = request.get(i).getAsJsonObject();
            assertEquals(MOVIE_IDS[i].longValue(), movie.get("movie_id").getAsLong());
            assertEquals(TITLES[i], movie.get("title").getAsString());
            assertArrayEquals(GENRES[i], toStringArray(movie.get("genres").getAsJsonArray()));
        }
    }

    /** batchSize smaller than the row count: rows must be chunked into batches. */
    @Test
    void testBatchingSplitsRowsIntoChunks() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        TableResult result = tEnv.executeSql(itemEmbeddingSql(serviceUrl, 2, 4));

        List<Row> rows = collectRows(result);
        assertEquals(4, rows.size(), "One output row per movie");

        assertEquals(2, receivedBodies.size(), "batchSize=2 over 4 rows should produce exactly 2 POSTs");
        for (String body : receivedBodies) {
            assertEquals(2, JsonParser.parseString(body).getAsJsonArray().size(),
                    "Each request should carry exactly batchSize rows");
        }

        Set<Long> ids = new HashSet<>();
        for (Row row : rows) {
            ids.add((Long) row.getField(0));
        }
        assertEquals(new HashSet<>(Arrays.asList(MOVIE_IDS).subList(0, 4)), ids);
    }

    /**
     * Columnar predictions of different value types are routed to the right
     * output map: decimals -> double_map, integers -> long_map, scalars are
     * broadcast to every row -> string_map.
     */
    @Test
    void testPredictionTypesAcrossMaps() throws Exception {
        JsonObject extra = new JsonObject();
        JsonArray scores = new JsonArray();
        scores.add(new JsonPrimitive(0.9));
        scores.add(new JsonPrimitive(0.8));
        JsonArray categories = new JsonArray();
        categories.add(new JsonPrimitive(101));
        categories.add(new JsonPrimitive(102));
        extra.add("score", scores);
        extra.add("category", categories);
        extra.addProperty("model_version", "v1.2");
        extraPredictions = extra;

        StreamTableEnvironment tEnv = createTableEnv();

        TableResult result = tEnv.executeSql(
                "SELECT \n" +
                "    r.long_map['movie_id'] AS id,\n" +
                "    r.double_map['score'] AS score,\n" +
                "    r.long_map['category'] AS category,\n" +
                "    r.string_map['model_version'] AS model_version\n" +
                "FROM (" + moviesValuesSql(2) + ") AS ml_movies (movie_id, title, genres), \n" +
                "LATERAL TABLE(batch_call_service(\n" +
                "    '" + serviceUrl + "',\n" +
                "    2, \n" +
                "    'movie_id', movie_id, \n" +
                "    'title', title, \n" +
                "    'genres', genres\n" +
                ")) AS r"
        );

        List<Row> rows = collectRows(result);
        assertEquals(2, rows.size());

        Map<Long, Row> byId = new HashMap<>();
        for (Row row : rows) {
            byId.put((Long) row.getField(0), row);
        }
        // Per-row columnar values align by position with the request rows.
        assertEquals(0.9, (Double) byId.get(1L).getField(1), 1e-9);
        assertEquals(0.8, (Double) byId.get(2L).getField(1), 1e-9);
        assertEquals(101L, byId.get(1L).getField(2));
        assertEquals(102L, byId.get(2L).getField(2));
        // Scalar value is broadcast to every row.
        assertEquals("v1.2", byId.get(1L).getField(3));
        assertEquals("v1.2", byId.get(2L).getField(3));
    }

    // ---- SQL helpers ----

    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // Same registration as load_features.sql
        tEnv.executeSql(
                "CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF'");
        return tEnv;
    }

    /** Builds the item_embedding SELECT from load_features.sql over inline movies. */
    private static String itemEmbeddingSql(String serviceUrl, int batchSize, int movieCount) {
        return "SELECT \n" +
                "    r.long_map['movie_id'] AS id,\n" +
                "    r.string_map['title'] AS title,\n" +
                "    r.string_array_map['genres'] AS genres,\n" +
                "    r.double_array_map['item_tower_emb'] AS embedding\n" +
                "FROM (" + moviesValuesSql(movieCount) + ") AS ml_movies (movie_id, title, genres), \n" +
                "LATERAL TABLE(batch_call_service(\n" +
                "    '" + serviceUrl + "',\n" +
                "    " + batchSize + ", \n" +
                "    'movie_id', movie_id, \n" +
                "    'title', title, \n" +
                "    'genres', genres\n" +
                ")) AS r";
    }

    private static String moviesValuesSql(int count) {
        StringBuilder sb = new StringBuilder("VALUES\n");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(String.format("    (%d, '%s', %s)", MOVIE_IDS[i], TITLES[i], genresLiteral(GENRES[i])));
        }
        return sb.toString();
    }

    private static String genresLiteral(String[] genres) {
        StringBuilder sb = new StringBuilder("ARRAY[");
        for (int i = 0; i < genres.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('\'').append(genres[i]).append('\'');
        }
        return sb.append(']').toString();
    }

    // ---- Helpers ----

    private static List<Row> collectRows(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    private static String[] toStringArray(JsonArray array) {
        String[] result = new String[array.size()];
        for (int i = 0; i < array.size(); i++) {
            result[i] = array.get(i).getAsString();
        }
        return result;
    }
}
