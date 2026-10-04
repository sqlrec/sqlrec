package com.sqlrec.common.http;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdkJsonHttpTransportTest {

    @Test
    void postsUtf8JsonAndPreservesThePredictionHeaders() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> received = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> accept = new AtomicReference<>();
        AtomicReference<String> method = new AtomicReference<>();
        server.createContext("/predict", exchange -> {
            received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            accept.set(exchange.getRequestHeaders().getFirst("Accept"));
            method.set(exchange.getRequestMethod());
            byte[] response = "{\"label\":\"推荐\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        server.start();
        try {
            String body = "[{\"title\":\"电影\"}]";
            assertEquals("{\"label\":\"推荐\"}", JdkJsonHttpTransport.post(
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/predict", body, 1000, 1000, 1000));
            assertEquals(body, received.get());
            assertEquals("POST", method.get());
            assertEquals("application/json", accept.get());
            assertEquals("application/json; charset=utf-8", contentType.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void errorStatusIsReportedAndTheConnectionIsReleased() throws Exception {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        when(connection.getOutputStream()).thenReturn(new ByteArrayOutputStream());
        when(connection.getResponseCode()).thenReturn(503);
        assertEquals("HTTP request failed with response code: 503", assertThrows(RuntimeException.class,
                () -> JdkJsonHttpTransport.post(connection, "{}", 1000, 1000, 1000)).getMessage());
        verify(connection).disconnect();
        verify(connection, never()).getInputStream();
    }

    @Test
    void readTimeoutPropagatesAndDisconnectsTheConnection() throws Exception {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        when(connection.getOutputStream()).thenReturn(new ByteArrayOutputStream());
        when(connection.getResponseCode()).thenReturn(200);
        SocketTimeoutException failure = new SocketTimeoutException("read timed out");
        when(connection.getInputStream()).thenThrow(failure);
        assertSame(failure, assertThrows(SocketTimeoutException.class,
                () -> JdkJsonHttpTransport.post(connection, "{}", 1000, 50, 1000)));
        verify(connection).setConnectTimeout(1000);
        verify(connection).setReadTimeout(50);
        verify(connection).disconnect();
    }

    @Test
    void blockedWritesHaveTheirOwnTimeout() throws Exception {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        when(connection.getOutputStream()).thenReturn(new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("write interrupted", e);
                } finally {
                    finished.countDown();
                }
            }
        });
        try {
            assertEquals("HTTP request write timed out", assertThrows(SocketTimeoutException.class,
                    () -> JdkJsonHttpTransport.post(connection, "{}", 1000, 1000, 100)).getMessage());
            verify(connection).disconnect();
            verify(connection, never()).getResponseCode();
        } finally {
            release.countDown();
            assertTrue(finished.await(5, TimeUnit.SECONDS));
        }
    }
}
