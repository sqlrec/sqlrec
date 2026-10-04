package com.sqlrec.flink;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchCallServiceBatchingTest {
    @Test
    @Timeout(60)
    void returnedMapsPreserveFilteredInputsFullBatchAndTrailingRows() throws Exception {
        List<JsonArray> requests = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/predict", exchange -> {
            JsonArray request = JsonParser.parseString(new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).getAsJsonArray();
            requests.add(request);
            JsonArray embeddings = new JsonArray();
            for (JsonElement input : request) {
                JsonArray embedding = new JsonArray();
                embedding.add(input.getAsJsonObject().get("movie_id").getAsDouble() * 0.1);
                embeddings.add(embedding);
            }
            JsonObject response = new JsonObject();
            response.add("item_tower_emb", embeddings);
            byte[] bytes = response.toString().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (java.io.OutputStream stream = exchange.getResponseBody()) { stream.write(bytes); }
        });
        server.start();
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment table = StreamTableEnvironment.create(env);
            table.executeSql("CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF'");
            String sql = "SELECT r.long_map['movie_id'], r.string_map['title'], "
                    + "r.string_array_map['genres'], r.double_array_map['item_tower_emb'], "
                    + "r.double_map['genre_count'] FROM (SELECT * FROM (VALUES "
                    + "(1,'A',ARRAY['a','b'],'2024-01-01'),"
                    + "(2,'excluded',ARRAY['x'],'2024-01-02'),"
                    + "(3,'C',ARRAY['c'],'2024-01-01'),"
                    + "(4,'D',ARRAY['d'],'2024-01-01')) "
                    + "AS source(movie_id,title,genres,dt) WHERE dt = '2024-01-01') AS m, "
                    + "LATERAL TABLE(batch_call_service('http://localhost:" + server.getAddress().getPort()
                    + "/predict',2,'movie_id',movie_id,'title',title,'genres',genres,"
                    + "'genre_count',CAST(CARDINALITY(genres) AS FLOAT))) AS r";
            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> result = table.executeSql(sql).collect()) {
                result.forEachRemaining(rows::add);
            }
            rows.sort(java.util.Comparator.comparingLong(row -> (Long) row.getField(0)));
            assertEquals(3, rows.size());
            assertEquals(List.of(1L, 3L, 4L), rows.stream()
                    .map(row -> (Long) row.getField(0)).collect(java.util.stream.Collectors.toList()));
            for (int i = 0; i < rows.size(); i++) {
                Row row = rows.get(i);
                assertEquals(List.of("A", "C", "D").get(i), row.getField(1));
                assertArrayEquals(i == 0 ? new String[]{"a", "b"} : new String[]{i == 1 ? "c" : "d"},
                        (String[]) row.getField(2));
                assertArrayEquals(new Double[]{(Long) row.getField(0) * 0.1}, (Double[]) row.getField(3));
                assertEquals(i == 0 ? 2.0 : 1.0, row.getField(4));
            }
            assertEquals(List.of(2, 1), requests.stream().map(JsonArray::size)
                    .collect(java.util.stream.Collectors.toList()));
            assertEquals(List.of(1L, 3L, 4L), requests.stream()
                    .flatMap(request -> java.util.stream.StreamSupport.stream(request.spliterator(), false))
                    .map(input -> input.getAsJsonObject().get("movie_id").getAsLong())
                    .collect(java.util.stream.Collectors.toList()));
            for (JsonArray request : requests) {
                for (JsonElement input : request) {
                    JsonObject movie = input.getAsJsonObject();
                    assertEquals(movie.getAsJsonArray("genres").size(), movie.get("genre_count").getAsFloat());
                }
            }
        } finally { server.stop(0); }
    }
}
