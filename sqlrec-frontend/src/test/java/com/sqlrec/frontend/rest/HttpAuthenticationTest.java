package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import com.sqlrec.frontend.utils.RestUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class HttpAuthenticationTest {
    private static final String BASIC = "Basic c3FscmVjOnNlY3JldA=="; // sqlrec:secret
    private final Boolean previousEnabled = SqlRecConfigs.AUTH_ENABLED.getDefaultValue();
    private final String previousUsername = SqlRecConfigs.AUTH_USERNAME.getDefaultValue();
    private final String previousPassword = SqlRecConfigs.AUTH_PASSWORD.getDefaultValue();

    @BeforeEach
    void configureAuthentication() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(true);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("sqlrec");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("secret");
    }

    @AfterEach
    void restoreConfiguration() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(previousEnabled);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue(previousUsername);
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue(previousPassword);
    }

    @Test
    void protectsEveryRouteExceptExactMetricsGetBeforeParsingOrDispatch() {
        UiHandler ui = mock(UiHandler.class);
        try (var sql = mockStatic(RestSqlExecutor.class);
             var api = mockStatic(RestFunctionExecutor.class)) {
            EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(ui));
            try {
                for (String path : List.of("/sql/v1", "/api/v1/example", "/ui/api/functions",
                        "/ui/static/index.html", "/unknown", "/metrics/", "/metrics%2f", "/metrics/../sql/v1")) {
                    DefaultFullHttpRequest request = request(HttpMethod.POST, path, "{bad");
                    assertUnauthorized(channel, request);
                }
                assertUnauthorized(channel, request(HttpMethod.GET, "/ui/static/index.html", ""));
                assertUnauthorized(channel, request(HttpMethod.GET, "/ui/api/functions", ""));
                assertUnauthorized(channel, request(HttpMethod.POST, "/metrics", ""));
                verifyNoInteractions(ui);
                sql.verifyNoInteractions();
                api.verifyNoInteractions();
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void metricsRemainPublicIncludingQueryStringsAndBadAuthorization() {
        PrometheusMetricsUtils.initMetrics();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(mock(UiHandler.class)));
        try {
            for (String path : List.of("/metrics", "/metrics?format=prometheus")) {
                DefaultFullHttpRequest request = request(HttpMethod.GET, path, "");
                request.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic invalid");
                channel.writeInbound(request);
                FullHttpResponse response = channel.readOutbound();
                try {
                    assertEquals(HttpResponseStatus.OK, response.status());
                    assertEquals("text/plain", response.headers().get(HttpHeaderNames.CONTENT_TYPE));
                    assertNull(response.headers().get(HttpHeaderNames.WWW_AUTHENTICATE));
                } finally {
                    response.release();
                }
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsIncorrectAndDuplicateHeadersAndRechecksEachKeepAliveRequest() throws Exception {
        UiHandler ui = mock(UiHandler.class);
        when(ui.handleRequest(eq("/ui/api/functions"), anyMap())).thenReturn(RestUtils.ok("[]"));
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(ui));
        try {
            for (String header : List.of("Basic !!!", "Bearer token", "Basic c3FscmVjOndyb25n")) {
                DefaultFullHttpRequest request = request(HttpMethod.GET, "/ui/api/functions", "");
                request.headers().set(HttpHeaderNames.AUTHORIZATION, header);
                assertUnauthorized(channel, request);
            }
            DefaultFullHttpRequest duplicate = request(HttpMethod.GET, "/ui/api/functions", "");
            duplicate.headers().add(HttpHeaderNames.AUTHORIZATION, BASIC);
            duplicate.headers().add(HttpHeaderNames.AUTHORIZATION, BASIC);
            assertUnauthorized(channel, duplicate);

            DefaultFullHttpRequest valid = request(HttpMethod.GET, "/ui/api/functions", "");
            valid.headers().set(HttpHeaderNames.AUTHORIZATION, BASIC);
            channel.writeInbound(valid);
            FullHttpResponse response = channel.readOutbound();
            try {
                assertEquals(HttpResponseStatus.OK, response.status());
            } finally {
                response.release();
            }
            assertUnauthorized(channel, request(HttpMethod.GET, "/ui/api/functions", ""));
            verify(ui, times(1)).handleRequest("/ui/api/functions", Map.of());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void authenticatedPostsRequireJsonAndThenReachTheExecutor() throws Exception {
        try (var sql = mockStatic(RestSqlExecutor.class)) {
            sql.when(() -> RestSqlExecutor.execute("{\"sqls\":[\"SELECT 1\"]}"))
                    .thenReturn(new com.sqlrec.common.rest.ExecuteDataList());
            EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(mock(UiHandler.class)));
            try {
                for (String contentType : List.of("text/plain", "application/x-www-form-urlencoded", "multipart/form-data")) {
                    DefaultFullHttpRequest request = request(HttpMethod.POST, "/sql/v1", "{\"sqls\":[\"SELECT 1\"]}");
                    request.headers().set(HttpHeaderNames.AUTHORIZATION, BASIC);
                    request.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
                    channel.writeInbound(request);
                    FullHttpResponse response = channel.readOutbound();
                    try {
                        assertEquals(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, response.status());
                    } finally {
                        response.release();
                    }
                }
                sql.verifyNoInteractions();
                DefaultFullHttpRequest valid = request(HttpMethod.POST, "/sql/v1", "{\"sqls\":[\"SELECT 1\"]}");
                valid.headers().set(HttpHeaderNames.AUTHORIZATION, BASIC);
                valid.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                channel.writeInbound(valid);
                FullHttpResponse response = channel.readOutbound();
                try {
                    assertEquals(HttpResponseStatus.OK, response.status());
                } finally {
                    response.release();
                }
                sql.verify(() -> RestSqlExecutor.execute("{\"sqls\":[\"SELECT 1\"]}"), times(1));
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void executionParamsCannotDisableAuthentication() {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(mock(UiHandler.class)));
        try {
            assertUnauthorized(channel, request(HttpMethod.POST, "/sql/v1",
                    "{\"sqls\":[\"SELECT 1\"],\"params\":{\"AUTH_ENABLED\":\"false\"}}"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static DefaultFullHttpRequest request(HttpMethod method, String path, String body) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path,
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        return request;
    }

    private static void assertUnauthorized(EmbeddedChannel channel, DefaultFullHttpRequest request) {
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        try {
            assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
            assertEquals("Basic realm=\"SQLRec\", charset=\"UTF-8\"",
                    response.headers().get(HttpHeaderNames.WWW_AUTHENTICATE));
            assertTrue(response.content().toString(CharsetUtil.UTF_8).contains("authentication required"));
        } finally {
            response.release();
        }
    }
}
