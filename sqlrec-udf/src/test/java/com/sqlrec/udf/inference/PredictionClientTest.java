package com.sqlrec.udf.inference;

import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.MediaType;
import okio.Buffer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PredictionClientTest {
    @Test
    void appliesServiceTimeoutsAndPreservesTheInjectedClientAndRequestProtocol() {
        AtomicInteger requests = new AtomicInteger();
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(11, TimeUnit.SECONDS)
                .readTimeout(12, TimeUnit.SECONDS)
                .writeTimeout(13, TimeUnit.SECONDS)
                .addInterceptor(chain -> {
                    int requestIndex = requests.getAndIncrement();
                    assertEquals(requestIndex == 0 ? 7 : 11000, chain.connectTimeoutMillis());
                    assertEquals(requestIndex == 0 ? 8 : 12000, chain.readTimeoutMillis());
                    assertEquals(requestIndex == 0 ? 9 : 13000, chain.writeTimeoutMillis());
                    assertEquals("POST", chain.request().method());
                    assertEquals("application/json", chain.request().header("Accept"));
                    assertEquals("application/json; charset=utf-8", chain.request().body().contentType().toString());
                    Buffer body = new Buffer();
                    chain.request().body().writeTo(body);
                    assertEquals("[{\"id\":1}]", body.readUtf8());
                    return new Response.Builder().request(chain.request()).protocol(Protocol.HTTP_1_1)
                            .code(200).message("OK")
                            .body(ResponseBody.create("{\"id\":[1],\"score\":0.5}",
                                    MediaType.parse("application/json")))
                            .build();
                }).build();
        PredictionClient client = new PredictionClient(httpClient);
        Map<String, Object> response = client.predict("http://prediction", "[{\"id\":1}]", Map.of(
                "connect_timeout_ms", "7", "read_timeout_ms", "8", "write_timeout_ms", "9"));
        assertEquals(0.5, response.get("score"));
        client.predict("http://prediction", "[{\"id\":1}]", Map.of("unrelated", "value"));
        assertEquals(2, requests.get());
        assertEquals(11000, httpClient.connectTimeoutMillis());
        assertEquals(12000, httpClient.readTimeoutMillis());
        assertEquals(13000, httpClient.writeTimeoutMillis());
    }

    @Test
    void rejectsInvalidTimeoutsBeforeMakingARequest() {
        AtomicInteger requests = new AtomicInteger();
        OkHttpClient httpClient = new OkHttpClient.Builder().addInterceptor(chain -> {
            requests.incrementAndGet();
            throw new AssertionError("invalid timeout must not send a request");
        }).build();
        PredictionClient client = new PredictionClient(httpClient);
        for (String key : new String[]{"connect_timeout_ms", "read_timeout_ms", "write_timeout_ms"}) {
            for (String value : new String[]{"0", "-1", "invalid"}) {
                assertEquals(key + " must be a positive integer", assertThrows(IllegalArgumentException.class,
                        () -> client.predict("http://prediction", "[]", Map.of(key, value))).getMessage());
            }
        }
        assertEquals(0, requests.get());
    }
}
