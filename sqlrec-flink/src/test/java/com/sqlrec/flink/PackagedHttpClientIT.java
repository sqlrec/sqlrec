package com.sqlrec.flink;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PackagedHttpClientIT {

    @TempDir
    Path temporary;

    @Test
    void defaultPredictionClientWorksWithLegacyOkioFirstOnTheClasspath() throws Exception {
        URL sqlrec = Path.of(System.getProperty("sqlrec.shadedJar")).toUri().toURL();
        URL legacy = legacyOkioJar().toUri().toURL();

        String requestJson = "[{\"id\":1}]";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/predict", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            boolean valid = "POST".equals(exchange.getRequestMethod()) && requestJson.equals(body);
            byte[] response = "{\"score\":0.5}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(valid ? 200 : 400, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        server.start();

        // Keep the conflicting Hadoop ByteString first, as in the original
        // deployment, with no fallback to this test JVM's classpath.
        try (URLClassLoader fixed = new URLClassLoader(new URL[]{legacy, sqlrec},
                ClassLoader.getPlatformClassLoader())) {
            assertEquals(legacy, fixed.loadClass("okio.ByteString")
                    .getProtectionDomain().getCodeSource().getLocation());
            Object client = defaultClient(fixed);
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/predict";
            Map<?, ?> predictions = (Map<?, ?>) client.getClass()
                    .getMethod("predict", String.class, String.class).invoke(client, url, requestJson);
            assertEquals(0.5, predictions.get("score"));
        } finally {
            server.stop(0);
        }
    }

    private static Object defaultClient(ClassLoader loader) throws Exception {
        return loader.loadClass("com.sqlrec.udf.inference.PredictionClient")
                .getMethod("getDefault").invoke(null);
    }

    private Path legacyOkioJar() throws Exception {
        Path source = temporary.resolve("okio/ByteString.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "package okio; public final class ByteString {}");
        assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null,
                "-d", temporary.toString(), source.toString()));
        Path jar = temporary.resolve("flink-shaded-hadoop-2-uber.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry("okio/ByteString.class"));
            Files.copy(temporary.resolve("okio/ByteString.class"), output);
            output.closeEntry();
        }
        return jar;
    }
}
