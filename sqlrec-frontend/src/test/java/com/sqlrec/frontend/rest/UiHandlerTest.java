package com.sqlrec.frontend.rest;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UiHandlerTest {
    private static final Pattern SCRIPT_PATH = Pattern.compile("/ui/static/[^\"]+\\.js");

    @Test
    void servesStaticAssetWhenCacheBustingQueryStringIsPresent() throws Exception {
        ClassLoader classLoader = UiHandler.class.getClassLoader();
        String indexHtml;
        try (InputStream input = classLoader.getResourceAsStream("ui/static/index.html")) {
            assertNotNull(input);
            indexHtml = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }

        Matcher matcher = SCRIPT_PATH.matcher(indexHtml);
        assertTrue(matcher.find(), "index.html should reference a JavaScript asset");
        String assetUri = matcher.group();
        byte[] expected;
        try (InputStream input = classLoader.getResourceAsStream(assetUri.substring(1))) {
            assertNotNull(input);
            expected = input.readAllBytes();
        }

        FullHttpResponse response = UiHandler.handleRequest(assetUri + "?v=123", HttpMethod.GET, "");
        try {
            assertEquals(HttpResponseStatus.OK, response.status());
            assertEquals("application/javascript; charset=UTF-8", response.headers().get("Content-Type"));
            byte[] actual = new byte[response.content().readableBytes()];
            response.content().getBytes(response.content().readerIndex(), actual);
            assertArrayEquals(expected, actual);
        } finally {
            response.release();
        }
    }

    @Test
    void doesNotTreatQueryStringAsUiApiPath() {
        FullHttpResponse response = UiHandler.handleRequest("/ui/api/?page=1", HttpMethod.GET, "");
        try {
            assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        } finally {
            response.release();
        }
    }
}
