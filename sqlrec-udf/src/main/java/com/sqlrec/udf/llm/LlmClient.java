package com.sqlrec.udf.llm;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** OpenAI text and Ark multimodal protocols over a shared, dependency-free JDK client. */
public final class LlmClient {
    private LlmClient() {
    }

    // Lazy initialization keeps Hive initialization and SQL type inference free of network resources.
    private static final class Transport {
        static final HttpClient CLIENT = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        static final Semaphore SLOTS = new Semaphore(32, true);
    }

    public static List<Double> embedText(String endpoint, String apiKey, String model,
                                        String text, String optionsJson) {
        if (text == null) {
            return null;
        }
        requireNonEmpty(text, "text");
        LlmOptions options = LlmOptions.parse(optionsJson, "dimensions", "user");
        JsonObject body = body(model, options);
        body.addProperty("input", text);
        body.addProperty("encoding_format", "float");
        URI uri = endpoint(endpoint, apiKey);
        JsonObject result = post(uri, apiKey, body, options);
        JsonArray data = array(result.get("data"));
        if (data.size() != 1) {
            throw invalidResponse();
        }
        return vector(object(data.get(0)).get("embedding"), options);
    }

    public static String complete(String endpoint, String apiKey, String model,
                                  String prompt, String optionsJson) {
        if (prompt == null) {
            return null;
        }
        LlmOptions options = LlmOptions.parse(optionsJson, "temperature", "top_p",
                "max_completion_tokens", "reasoning_effort", "user");
        JsonObject body = body(model, options);
        JsonObject message = new JsonObject();
        message.addProperty("role", "user");
        message.addProperty("content", prompt);
        JsonArray messages = new JsonArray();
        messages.add(message);
        body.add("messages", messages);
        body.addProperty("stream", false);
        body.addProperty("n", 1);
        body.addProperty("store", false);
        URI uri = endpoint(endpoint, apiKey);
        JsonObject result = post(uri, apiKey, body, options);
        JsonArray choices = array(result.get("choices"));
        if (choices.size() != 1) {
            throw invalidResponse();
        }
        JsonObject choice = object(choices.get(0));
        JsonObject response = object(choice.get("message"));
        JsonElement finishReason = choice.get("finish_reason");
        if ((response.has("refusal") && !response.get("refusal").isJsonNull())
                || (response.has("tool_calls") && !response.get("tool_calls").isJsonNull())
                || (finishReason != null && !finishReason.isJsonNull()
                && "content_filter".equals(text(finishReason)))) {
            throw new RuntimeException("LLM response did not provide a text completion");
        }
        // A length-limited completion is returned as supplied by the API.
        return text(response.get("content"));
    }

    public static List<Double> embedImage(String endpoint, String apiKey, String model,
                                         String image, String optionsJson) {
        if (image == null) {
            return null;
        }
        requireNonEmpty(image, "image");
        if (!image.startsWith("data:image/")) {
            endpoint(image, "");
        } else if (!image.matches("(?s)data:image/[A-Za-z0-9.+-]+;base64,[A-Za-z0-9+/]+={0,2}")) {
            throw new IllegalArgumentException("image must be an HTTP(S) URL or an image Base64 data URI");
        }
        LlmOptions options = LlmOptions.parse(optionsJson, "dimensions", "instructions");
        if (options.request.has("dimensions")) {
            int dimensions = LlmOptions.positiveInt(options.request, "dimensions");
            if (dimensions != 1024 && dimensions != 2048) {
                throw new IllegalArgumentException("Ark dimensions must be 1024 or 2048");
            }
        }
        JsonObject body = body(model, options);
        JsonObject imageUrl = new JsonObject();
        imageUrl.addProperty("url", image);
        JsonObject input = new JsonObject();
        input.addProperty("type", "image_url");
        input.add("image_url", imageUrl);
        JsonArray inputs = new JsonArray();
        inputs.add(input);
        body.add("input", inputs);
        body.addProperty("encoding_format", "float");
        URI uri = endpoint(endpoint, apiKey);
        JsonObject result = post(uri, apiKey, body, options);
        return vector(object(result.get("data")).get("embedding"), options);
    }

    private static JsonObject body(String model, LlmOptions options) {
        requireNonEmpty(model, "model");
        JsonObject body = options.request.deepCopy();
        body.addProperty("model", model);
        return body;
    }

    private static URI endpoint(String endpoint, String apiKey) {
        requireNonEmpty(endpoint, "endpoint");
        try {
            URI uri = URI.create(endpoint);
            if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getPort() > 65535
                    || uri.getUserInfo() != null || uri.getFragment() != null) {
                throw new IllegalArgumentException();
            }
            if (apiKey == null || apiKey.chars().anyMatch(c -> c <= 32 || c >= 127)) {
                throw new IllegalArgumentException();
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid HTTP(S) endpoint or api_key");
        }
    }

    private static void requireNonEmpty(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    private static JsonObject post(URI uri, String apiKey, JsonObject body, LlmOptions options) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.timeoutMs);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(options.timeoutMs))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        if (!apiKey.isEmpty()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        boolean acquired = false;
        CompletableFuture<HttpResponse<String>> pending = null;
        try {
            acquired = Transport.SLOTS.tryAcquire(remaining(deadline), TimeUnit.NANOSECONDS);
            if (!acquired) {
                throw new TimeoutException();
            }
            pending = Transport.CLIENT.sendAsync(builder.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            HttpResponse<String> response = pending.get(remaining(deadline), TimeUnit.NANOSECONDS);
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new RuntimeException("LLM HTTP request failed (status " + response.statusCode() + ")");
            }
            JsonElement parsed;
            try {
                parsed = JsonParser.parseString(response.body());
            } catch (JsonParseException e) {
                throw invalidResponse();
            }
            JsonObject result = object(parsed);
            if (result.has("error")) {
                throw invalidResponse();
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("LLM request interrupted");
        } catch (TimeoutException e) {
            throw new RuntimeException("LLM request timed out");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof HttpTimeoutException) {
                throw new RuntimeException("LLM request timed out");
            }
            throw new RuntimeException("LLM HTTP transport failed");
        } finally {
            if (pending != null && !pending.isDone()) {
                pending.cancel(true);
            }
            if (acquired) {
                Transport.SLOTS.release();
            }
        }
    }

    private static long remaining(long deadline) throws TimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new TimeoutException();
        }
        return remaining;
    }

    private static List<Double> vector(JsonElement element, LlmOptions options) {
        if (element == null || !element.isJsonArray() || element.getAsJsonArray().size() == 0) {
            throw invalidResponse();
        }
        List<Double> vector = new ArrayList<>();
        for (JsonElement entry : element.getAsJsonArray()) {
            if (!entry.isJsonPrimitive() || !entry.getAsJsonPrimitive().isNumber()) {
                throw invalidResponse();
            }
            double value = entry.getAsDouble();
            if (!Double.isFinite(value)) {
                throw invalidResponse();
            }
            vector.add(value);
        }
        if (options.request.has("dimensions")
                && vector.size() != LlmOptions.positiveInt(options.request, "dimensions")) {
            throw new RuntimeException("LLM embedding dimension does not match requested dimensions");
        }
        return vector;
    }

    private static JsonObject object(JsonElement element) {
        if (element == null || !element.isJsonObject()) {
            throw invalidResponse();
        }
        return element.getAsJsonObject();
    }

    private static JsonArray array(JsonElement element) {
        if (element == null || !element.isJsonArray()) {
            throw invalidResponse();
        }
        return element.getAsJsonArray();
    }

    private static String text(JsonElement element) {
        if (element == null || !element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
            throw invalidResponse();
        }
        return element.getAsString();
    }

    private static RuntimeException invalidResponse() {
        return new RuntimeException("Invalid LLM response");
    }
}
