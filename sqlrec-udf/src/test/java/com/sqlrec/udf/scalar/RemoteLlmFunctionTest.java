package com.sqlrec.udf.scalar;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RemoteLlmFunctionTest {
    private HttpServer server;
    private String endpoint;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<JsonObject> request = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private String response = "{\"data\":[{\"embedding\":[3,4]}]}";
    private int status = 200;
    private final LlmEmbedTextFunction embed = new LlmEmbedTextFunction();
    private final LlmCompleteFunction complete = new LlmCompleteFunction();
    private final ArkEmbedImageFunction image = new ArkEmbedImageFunction();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api", exchange -> {
            requests.incrementAndGet();
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            request.set(JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8)).getAsJsonObject());
            respond(exchange, response, status);
        });
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/api";
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void openAiEmbeddingUsesFloatEncodingAndKeepsRuntimeOptionsLocal() {
        assertEquals(Arrays.asList(3.0, 4.0), embed.evaluate(endpoint, "secret-one", "model-a", "中文文本",
                "{\"request\":{\"dimensions\":2},\"runtime\":{\"timeout_ms\":5000}}"));
        assertEquals("Bearer secret-one", authorization.get());
        assertEquals("中文文本", request.get().get("input").getAsString());
        assertEquals("float", request.get().get("encoding_format").getAsString());
        assertEquals(2, request.get().get("dimensions").getAsInt());
        assertFalse(request.get().has("runtime"));
        assertFalse(request.get().has("timeout_ms"));
        assertFalse(request.get().has("api_key"));
        embed.evaluate(endpoint, "secret-two", "model-b", "next", "{}");
        assertEquals("Bearer secret-two", authorization.get());
        assertEquals("model-b", request.get().get("model").getAsString());
        embed.evaluate(endpoint, "", "model-b", "next", "{}");
        assertNull(authorization.get());
    }

    @Test
    void completionUsesSingleNonStreamingUserMessageWithoutModelSpecificDefaults() {
        response = "{\"choices\":[{\"message\":{\"content\":\"摘要\",\"refusal\":null},\"finish_reason\":\"stop\"}]}";
        assertEquals("摘要", complete.evaluate(endpoint, "key", "model", "总结这段文字", "{}"));
        JsonObject body = request.get();
        assertEquals("user", body.getAsJsonArray("messages").get(0).getAsJsonObject().get("role").getAsString());
        assertEquals("总结这段文字", body.getAsJsonArray("messages").get(0).getAsJsonObject().get("content").getAsString());
        assertFalse(body.get("stream").getAsBoolean());
        assertFalse(body.get("store").getAsBoolean());
        assertEquals(1, body.get("n").getAsInt());
        assertFalse(body.has("temperature"));
        assertFalse(body.has("max_completion_tokens"));
        complete.evaluate(endpoint, "key", "model", "text",
                "{\"request\":{\"temperature\":0,\"max_completion_tokens\":128}}");
        assertEquals(128, request.get().get("max_completion_tokens").getAsInt());
    }

    @Test
    void arkUsesTypedImageInputAndObjectResponseForUrlsAndBase64() {
        response = "{\"data\":{\"embedding\":[0.5,0.25]}}";
        assertEquals(Arrays.asList(0.5, 0.25), image.evaluate(endpoint, "key", "vision", "https://example.com/item.png",
                "{\"request\":{\"instructions\":\"Represent the image\"}}"));
        JsonObject input = request.get().getAsJsonArray("input").get(0).getAsJsonObject();
        assertEquals("image_url", input.get("type").getAsString());
        assertEquals("https://example.com/item.png", input.getAsJsonObject("image_url").get("url").getAsString());
        assertEquals("Represent the image", request.get().get("instructions").getAsString());
        String dataUri = "data:image/png;base64,aGVsbG8=";
        image.evaluate(endpoint, "key", "vision", dataUri, "{}");
        assertEquals(dataUri, request.get().getAsJsonArray("input").get(0).getAsJsonObject()
                .getAsJsonObject("image_url").get("url").getAsString());
    }

    @Test
    void nullInputsSkipValidationAndAllNetworkCalls() {
        assertNull(embed.evaluate(null, null, null, null, "broken"));
        assertNull(complete.evaluate(null, null, null, null, "broken"));
        assertNull(image.evaluate(null, null, null, null, "broken"));
        assertEquals(0, requests.get());
    }

    @Test
    void invalidOptionsAndReservedFieldsFailLocally() {
        for (String options : Arrays.asList("", "null", "[]", "broken", "{request:{}}", "{} trailing",
                "{\"request\":null}", "{\"runtime\":{\"timeout_ms\":0}}",
                "{\"runtime\":{\"timeout_ms\":1.1}}", "{\"runtime\":{\"timeout_ms\":300001}}",
                "{\"runtime\":{\"on_error\":\"null\"}}", "{\"runtime\":{\"on_error\":\"error\"}}",
                "{\"runtime\":{\"retries\":1}}",
                "{\"request\":{\"model\":\"override\"}}", "{\"request\":{\"encoding_format\":\"base64\"}}",
                "{\"request\":{\"dimensions\":\"2\"}}", "{\"request\":{\"dimensions\":-1}}",
                "{\"request\":{\"input\":\"override\"}}")) {
            assertThrows(IllegalArgumentException.class,
                    () -> embed.evaluate(endpoint, "key", "model", "text", options), options);
        }
        assertThrows(IllegalArgumentException.class, () -> complete.evaluate(endpoint, "key", "model", "prompt",
                "{\"request\":{\"temperature\":3}}"));
        assertThrows(IllegalArgumentException.class, () -> complete.evaluate(endpoint, "key", "model", "prompt",
                "{\"request\":{\"top_p\":\"0.5\"}}"));
        assertThrows(IllegalArgumentException.class, () -> image.evaluate(endpoint, "key", "vision", "file:///tmp/image", "{}"));
        assertThrows(IllegalArgumentException.class, () -> image.evaluate(endpoint, "key", "vision", "data:image/png;base64,broken?", "{}"));
        assertThrows(IllegalArgumentException.class, () -> image.evaluate(endpoint, "key", "vision", "https://example.com/x",
                "{\"request\":{\"dimensions\":3}}"));
        assertThrows(IllegalArgumentException.class, () -> embed.evaluate("file:///tmp/request", "key", "model", "text", "{}"));
        assertThrows(IllegalArgumentException.class, () -> embed.evaluate(endpoint, "secret\r\ninjected", "model", "text", "{}"));
        assertThrows(IllegalArgumentException.class, () -> embed.evaluate(endpoint, "key", "model", "", "{}"));
        assertEquals(0, requests.get());
    }

    @Test
    void malformedEmbeddingResponsesAndDimensionMismatchesAreRejected() {
        for (String bad : Arrays.asList("not json", "null", "[]", "{}", "{\"data\":[]}",
                "{\"data\":{}}", "{\"data\":[null]}", "{\"data\":[1]}",
                "{\"data\":[{\"embedding\":[]}]}", "{\"data\":[{\"embedding\":[null]}]}",
                "{\"data\":[{\"embedding\":[\"1\"]}]}", "{\"data\":[{\"embedding\":[1e999]}]}",
                "{\"data\":[{\"embedding\":\"base64\"}]}", "{\"error\":{\"message\":\"secret\"}}")) {
            response = bad;
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> embed.evaluate(endpoint, "key", "model", "text", "{}"));
            assertEquals("Invalid LLM response", failure.getMessage());
        }
        response = "{\"data\":[{\"embedding\":[1,2]}]}";
        RuntimeException textMismatch = assertThrows(RuntimeException.class,
                () -> embed.evaluate(endpoint, "key", "model", "text",
                        "{\"request\":{\"dimensions\":3}}"));
        assertEquals("LLM embedding dimension does not match requested dimensions", textMismatch.getMessage());
        response = "{\"data\":{\"embedding\":[1,2]}}";
        RuntimeException imageMismatch = assertThrows(RuntimeException.class,
                () -> image.evaluate(endpoint, "key", "model", "https://example.com/x",
                        "{\"request\":{\"dimensions\":1024}}"));
        assertEquals("LLM embedding dimension does not match requested dimensions", imageMismatch.getMessage());
        response = "{\"data\":[{\"embedding\":[1,2]}]}";
        RuntimeException invalidImageResponse = assertThrows(RuntimeException.class,
                () -> image.evaluate(endpoint, "key", "model", "https://example.com/x", "{}"));
        assertEquals("Invalid LLM response", invalidImageResponse.getMessage());
    }

    @Test
    void refusalsAndMissingCompletionTextAreNotSilentlyTurnedIntoStrings() {
        for (String bad : Arrays.asList("{\"choices\":[]}", "{\"choices\":[{\"message\":{\"content\":null}}]}",
                "{\"choices\":[{\"message\":{\"content\":12}}]}",
                "{\"choices\":[{\"message\":null}]}",
                "{\"choices\":[{\"message\":{\"content\":\"text\"},\"finish_reason\":[]}]}")) {
            response = bad;
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> complete.evaluate(endpoint, "key", "model", "text", "{}"));
            assertEquals("Invalid LLM response", failure.getMessage());
        }
        for (String refusal : Arrays.asList(
                "{\"choices\":[{\"message\":{\"content\":\"\",\"refusal\":\"refused\"}}]}",
                "{\"choices\":[{\"message\":{\"content\":\"\"},\"finish_reason\":\"content_filter\"}]}")) {
            response = refusal;
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> complete.evaluate(endpoint, "key", "model", "text", "{}"));
            assertEquals("LLM response did not provide a text completion", failure.getMessage());
        }
        response = "{\"choices\":[{\"message\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}";
        assertEquals("partial", complete.evaluate(endpoint, "key", "model", "text", null));
    }

    @Test
    void remoteFailuresThrowForEveryFunctionWithoutExposingResponseBodiesOrRetrying() {
        response = "{\"error\":{\"message\":\"secret-key private-input\"}}";
        for (int code : new int[]{401, 429, 503}) {
            status = code;
            RuntimeException embeddingFailure = assertThrows(RuntimeException.class,
                    () -> embed.evaluate(endpoint, "secret-key", "model", "private-input", "{}"));
            RuntimeException completionFailure = assertThrows(RuntimeException.class,
                    () -> complete.evaluate(endpoint, "secret-key", "model", "private-input", "{}"));
            RuntimeException imageFailure = assertThrows(RuntimeException.class,
                    () -> image.evaluate(endpoint, "secret-key", "model", "https://example.com/x", "{}"));
            for (RuntimeException failure : Arrays.asList(embeddingFailure, completionFailure, imageFailure)) {
                assertEquals("LLM HTTP request failed (status " + code + ")", failure.getMessage());
                assertNull(failure.getCause());
            }
        }
        assertEquals(9, requests.get());
    }

    @Test
    void hiveBridgePropagatesRemoteFailuresAsHiveExceptions() throws Exception {
        status = 503;
        response = "{\"error\":{\"message\":\"service unavailable\"}}";
        for (RemoteLlmFunction function : Arrays.asList(embed, complete, image)) {
            ObjectInspector[] inspectors = new ObjectInspector[5];
            Arrays.fill(inspectors, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            function.initialize(inspectors);
            String input = function instanceof ArkEmbedImageFunction ? "https://example.com/x" : "text";
            HiveException failure = assertThrows(HiveException.class,
                    () -> function.evaluate(deferred(endpoint, "key", "model", input, "{}")));
            assertInstanceOf(RuntimeException.class, failure.getCause());
            assertEquals("LLM HTTP request failed (status 503)", failure.getCause().getMessage());
        }
        assertEquals(3, requests.get());
    }

    @Test
    void redirectsAreNotFollowed() {
        server.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().add("Location", endpoint);
            respond(exchange, "", 302);
        });
        assertThrows(RuntimeException.class,
                () -> embed.evaluate(endpoint.replace("/api", "/redirect"), "key", "model", "text", "{}"));
        assertEquals(0, requests.get());
    }

    @Test
    void deadlineIncludesResponseBodyAndInterruptedRequestsKeepInterruptFlag() throws Exception {
        // Warm the shared client before testing a deliberately stalled response body.
        embed.evaluate(endpoint, "key", "model", "warmup", "{}");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/slow", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, 1000);
            exchange.getResponseBody().write('{');
            exchange.getResponseBody().flush();
            started.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        String slow = endpoint.replace("/api", "/slow");
        try {
            RuntimeException timeout = assertThrows(RuntimeException.class,
                    () -> embed.evaluate(slow, "key", "model", "text", "{\"runtime\":{\"timeout_ms\":300}}"));
            assertEquals("LLM request timed out", timeout.getMessage());
            assertEquals(0, started.getCount());
        } finally {
            release.countDown();
        }

        CountDownLatch waiting = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        server.createContext("/interrupt", exchange -> {
            exchange.getRequestBody().readAllBytes();
            waiting.countDown();
            try {
                finish.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                embed.evaluate(endpoint.replace("/api", "/interrupt"), "key", "model", "text", "{}");
            } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        caller.start();
        try {
            assertTrue(waiting.await(3, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(2000);
            assertFalse(caller.isAlive());
            assertInstanceOf(RuntimeException.class, failure.get());
            assertEquals("LLM request interrupted", failure.get().getMessage());
            assertEquals(Boolean.TRUE, interrupted.get());
        } finally {
            caller.interrupt();
            finish.countDown();
            caller.join(2000);
        }
    }

    @Test
    void hiveInitializationIsNonDeterministicAndConvertsWritableStrings() throws Exception {
        for (RemoteLlmFunction function : Arrays.asList(embed, complete, image)) {
            ObjectInspector[] inspectors = new ObjectInspector[5];
            Arrays.fill(inspectors, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            String expected = function instanceof LlmCompleteFunction ? "string" : "array<double>";
            assertEquals(expected, function.initialize(inspectors).getTypeName());
            assertFalse(function.getClass().getAnnotation(UDFType.class).deterministic());
            assertFalse(function.getDisplayString(new String[]{"endpoint", "secret"}).contains("secret"));
            assertNull(function.evaluate(deferred(endpoint, "key", "model", null, "{}")));
        }
        assertEquals(0, requests.get());
        assertEquals(Arrays.asList(3.0, 4.0), embed.evaluate(deferred(endpoint, "key", "model", "文本", "{}")));
        assertEquals("文本", request.get().get("input").getAsString());
        response = "{\"choices\":[{\"message\":{\"content\":\"text\"}}]}";
        assertEquals("text", complete.evaluate(deferred(endpoint, "key", "model", "prompt", "{}")));
        response = "{\"data\":{\"embedding\":[1,2]}}";
        assertEquals(Arrays.asList(1.0, 2.0), image.evaluate(deferred(endpoint, "key", "vision", "https://example.com/x", "{}")));
        assertThrows(UDFArgumentException.class, () -> embed.initialize(new ObjectInspector[0]));
        ObjectInspector[] invalid = new ObjectInspector[5];
        Arrays.fill(invalid, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        assertThrows(UDFArgumentException.class, () -> embed.initialize(invalid));
        assertEquals(LlmEmbedTextFunction.class.getName(), FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get("llm_embed_text"));
        assertEquals(LlmCompleteFunction.class.getName(), FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get("llm_complete"));
        assertEquals(ArkEmbedImageFunction.class.getName(), FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.get("ark_embed_image"));
    }

    private GenericUDF.DeferredObject[] deferred(String... strings) {
        return Arrays.stream(strings).map(value -> new GenericUDF.DeferredJavaObject(value == null ? null : new Text(value)))
                .toArray(GenericUDF.DeferredObject[]::new);
    }

    private static void respond(HttpExchange exchange, String body, int status) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try {
            exchange.getResponseBody().write(bytes);
        } finally {
            exchange.close();
        }
    }
}
