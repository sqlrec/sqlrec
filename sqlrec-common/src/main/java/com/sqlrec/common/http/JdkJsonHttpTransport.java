package com.sqlrec.common.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** JSON POST without third-party HTTP classes on the Flink shared classpath. */
public final class JdkJsonHttpTransport {
    private static final ExecutorService WRITERS = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "sqlrec-prediction-http-writer");
        thread.setDaemon(true);
        return thread;
    });

    private JdkJsonHttpTransport() {
    }

    public static String post(String url, String bodyJson,
                              int connectTimeoutMs, int readTimeoutMs, int writeTimeoutMs) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        return post(connection, bodyJson, connectTimeoutMs, readTimeoutMs, writeTimeoutMs);
    }

    static String post(HttpURLConnection connection, String bodyJson,
                       int connectTimeoutMs, int readTimeoutMs, int writeTimeoutMs) throws IOException {
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        connection.setConnectTimeout(connectTimeoutMs);
        connection.setReadTimeout(readTimeoutMs);
        connection.setDoOutput(true);
        byte[] body = bodyJson.getBytes(StandardCharsets.UTF_8);
        connection.setFixedLengthStreamingMode(body.length);

        boolean complete = false;
        try {
            connection.connect();
            writeBody(connection, body, writeTimeoutMs);
            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new RuntimeException("HTTP request failed with response code: " + status);
            }
            try (InputStream input = connection.getInputStream()) {
                String response = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                complete = true;
                return response;
            }
        } finally {
            // A fully consumed and closed response can reuse the JDK keep-alive
            // connection. Failed requests must release their socket immediately.
            if (!complete) {
                connection.disconnect();
            }
        }
    }

    private static void writeBody(HttpURLConnection connection, byte[] body, int timeoutMs) throws IOException {
        // HttpURLConnection exposes connect/read timeouts but no write timeout.
        // Bound the streaming write separately so write_timeout_ms still applies.
        Future<?> write = WRITERS.submit(() -> {
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body);
            }
            return null;
        });
        try {
            write.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            write.cancel(true);
            SocketTimeoutException failure = new SocketTimeoutException("HTTP request write timed out");
            failure.initCause(e);
            throw failure;
        } catch (InterruptedException e) {
            write.cancel(true);
            Thread.currentThread().interrupt();
            InterruptedIOException failure = new InterruptedIOException("HTTP request interrupted");
            failure.initCause(e);
            throw failure;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException("Failed to write HTTP request", cause);
        }
    }
}
