package com.sqlrec.frontend.rest;

import com.sqlrec.db.MetadataAccess;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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

        UiHandler handler = new UiHandler(mock(MetadataAccess.class));
        FullHttpResponse response = handler.handleRequest(assetUri + "?v=123");
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
    void doesNotTreatQueryStringAsUiApiPath() throws Exception {
        UiHandler handler = new UiHandler(mock(MetadataAccess.class));
        FullHttpResponse response = handler.handleRequest("/ui/api/?page=1");
        try {
            assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        } finally {
            response.release();
        }
    }

    @Test
    void servesIndexForUiRootAndSpaRoute() throws Exception {
        UiHandler handler = new UiHandler(mock(MetadataAccess.class));

        FullHttpResponse rootResponse = handler.handleRequest("/ui/static/");
        try {
            assertEquals(HttpResponseStatus.OK, rootResponse.status());
            assertEquals("text/html; charset=UTF-8", rootResponse.headers().get("Content-Type"));
            assertEquals("no-cache", rootResponse.headers().get("Cache-Control"));
        } finally {
            rootResponse.release();
        }

        FullHttpResponse routeResponse = handler.handleRequest("/ui/static/functions/example");
        try {
            assertEquals(HttpResponseStatus.OK, routeResponse.status());
            assertEquals("text/html; charset=UTF-8", routeResponse.headers().get("Content-Type"));
        } finally {
            routeResponse.release();
        }
    }

    @Test
    void returnsNotFoundForMissingStaticAsset() throws Exception {
        UiHandler handler = new UiHandler(mock(MetadataAccess.class));
        FullHttpResponse response = handler.handleRequest("/ui/static/missing.js");
        try {
            assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        } finally {
            response.release();
        }
    }

    @Test
    void rejectsEncodedStaticResourceTraversal() throws Exception {
        UiHandler handler = new UiHandler(mock(MetadataAccess.class));
        FullHttpResponse response = handler.handleRequest(
                "/ui/static/%2e%2e/config/database.properties");
        try {
            assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
        } finally {
            response.release();
        }
    }

    @Test
    void validatesCheckpointPaginationBeforeAccessingMetadata() throws Exception {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        UiHandler handler = new UiHandler(metadataAccess);

        FullHttpResponse invalidPage = handler.handleRequest(
                "/ui/api/models/model/checkpoints?page=0");
        try {
            assertEquals(HttpResponseStatus.BAD_REQUEST, invalidPage.status());
        } finally {
            invalidPage.release();
        }

        FullHttpResponse excessivePageSize = handler.handleRequest(
                "/ui/api/models/model/checkpoints?pageSize=101");
        try {
            assertEquals(HttpResponseStatus.BAD_REQUEST, excessivePageSize.status());
        } finally {
            excessivePageSize.release();
        }

        verifyNoInteractions(metadataAccess);
    }

    @Test
    void returnsNotFoundForMissingModelAndCheckpoint() throws Exception {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        UiHandler handler = new UiHandler(metadataAccess);

        FullHttpResponse modelResponse = handler.handleRequest("/ui/api/models/missing");
        try {
            assertEquals(HttpResponseStatus.NOT_FOUND, modelResponse.status());
        } finally {
            modelResponse.release();
        }

        FullHttpResponse checkpointResponse = handler.handleRequest(
                "/ui/api/models/missing/checkpoints/missing");
        try {
            assertEquals(HttpResponseStatus.NOT_FOUND, checkpointResponse.status());
        } finally {
            checkpointResponse.release();
        }
    }

    @Test
    void usesDefaultCheckpointPagination() throws Exception {
        MetadataAccess metadataAccess = mock(MetadataAccess.class);
        when(metadataAccess.getCheckpointListByModelNamePaged("model", 1, 10))
                .thenReturn(List.of());
        UiHandler handler = new UiHandler(metadataAccess);

        FullHttpResponse response = handler.handleRequest(
                "/ui/api/models/model/checkpoints");
        try {
            assertEquals(HttpResponseStatus.OK, response.status());
        } finally {
            response.release();
        }

        verify(metadataAccess).getCheckpointCountByModelName("model");
        verify(metadataAccess).getCheckpointListByModelNamePaged("model", 1, 10);
    }

    @Test
    void returnsNotFoundForMissingTable() throws Exception {
        UiHandler handler = new UiHandler(mock(MetadataAccess.class));
        FullHttpResponse response = handler.handleRequest(
                "/ui/api/tables/default/missing");
        try {
            assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
        } finally {
            response.release();
        }
    }
}
