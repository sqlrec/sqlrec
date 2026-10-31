package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.utils.RestUtils;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HttpServerHandlerTest {

    @Test
    void groupsUiAndUnknownPathsButPreservesApiPathsAndMethods() throws Exception {
        UiHandler ui = mock(UiHandler.class);
        when(ui.handleRequest(anyString(), anyMap())).thenAnswer(invocation -> RestUtils.ok("[]"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsUtils.getCompositeMeterRegistry().add(registry);
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(ui));
        try {
            for (int i = 0; i < 100; i++) {
                for (String path : List.of("/unknown/" + i, "/api/v1/api_" + (i % 2) + "?trace=" + i,
                        "/ui/api/models/model_" + i, "/ui/static/asset_" + i + ".js")) {
                    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path));
                    FullHttpResponse response = channel.readOutbound();
                    assertNotNull(response);
                    response.release();
                }
                channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                        HttpMethod.valueOf("CUSTOM" + i), "/unknown/" + i));
                FullHttpResponse response = channel.readOutbound();
                assertNotNull(response);
                response.release();
            }
            channel.runPendingTasks();
            var timers = registry.find(Consts.METRICS_HTTP_REQUEST_DURATION).timers();
            Set<String> paths = timers.stream().filter(timer -> timer.count() > 0)
                    .map(timer -> timer.getId().getTag("path"))
                    .collect(Collectors.toSet());
            assertTrue(Set.of("/api/v1/api_0", "/api/v1/api_1", "/ui/static/*", "/ui/api/*", "unmatched")
                    .containsAll(paths), paths.toString());
            for (int i = 0; i < 100; i++) {
                assertEquals(1, registry.find(Consts.METRICS_HTTP_REQUEST_DURATION)
                        .tags("path", "unmatched", "method", "CUSTOM" + i, "status", "404").timer().count());
            }
            for (int i = 0; i < 2; i++) {
                assertEquals(50, registry.find(Consts.METRICS_HTTP_REQUEST_DURATION)
                        .tags("path", "/api/v1/api_" + i, "method", "GET", "status", "405").timer().count());
            }
        } finally {
            channel.finishAndReleaseAll();
            MetricsUtils.getCompositeMeterRegistry().remove(registry);
            registry.close();
        }
    }

    @Test
    void routesMetricsRequestByPathWhenQueryStringIsPresent() {
        PrometheusMetricsUtils.initMetrics();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics?format=prometheus"));

            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(HttpResponseStatus.OK, response.status());
                assertEquals("text/plain", response.headers().get("Content-Type"));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void doesNotTreatQueryStringAsApiName() {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/v1/?trace=true"));

            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                Map<String, Object> body = JsonUtils.parseJsonToMap(
                        response.content().toString(CharsetUtil.UTF_8));
                assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
                assertEquals("api name is required", body.get("msg"));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void returnsMethodNotAllowedForKnownPath() {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, "/metrics"));

            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
                assertEquals(HttpMethod.GET.name(), response.headers().get(HttpHeaderNames.ALLOW));
                assertEquals("method POST is not allowed for /metrics; use GET", message(response));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void doesNotRouteSqlSubPathToSqlExecutor() {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, "/sql/v1/unexpected"));

            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(HttpResponseStatus.NOT_FOUND, response.status());
                assertEquals("path not found: /sql/v1/unexpected", message(response));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void returnsRequestDetailsForInvalidSqlJson() {
        assertError(HttpMethod.POST, "/sql/v1", "{bad", HttpResponseStatus.BAD_REQUEST,
                "invalid JSON request body:");
    }

    @Test
    void returnsRequestDetailsForMissingFunctionBody() {
        assertError(HttpMethod.POST, "/api/v1/example", "", HttpResponseStatus.BAD_REQUEST,
                "request body is required");
    }

    @Test
    @SilenceLoggers(HttpServerHandler.class)
    void returnsExceptionMessageForUnexpectedUiFailure() throws Exception {
        UiHandler uiHandler = mock(UiHandler.class);
        when(uiHandler.handleRequest(eq("/ui/api/functions"), anyMap()))
                .thenThrow(new IllegalStateException("metadata store unavailable"));
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler(uiHandler));
        try {
            channel.writeInbound(new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, "/ui/api/functions"));
            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
                assertEquals("metadata store unavailable", message(response));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static void assertError(HttpMethod method, String path, String body,
                                    HttpResponseStatus status, String messagePrefix) {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path,
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8)));
            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(status, response.status());
                assertTrue(message(response).startsWith(messagePrefix), message(response));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static String message(FullHttpResponse response) {
        return (String) JsonUtils.parseJsonToMap(
                response.content().toString(CharsetUtil.UTF_8)).get("msg");
    }

    @Test
    void keepsConnectionAliveWhenRequested() {
        PrometheusMetricsUtils.initMetrics();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpServerHandler());
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics");
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            channel.writeInbound(request);

            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response);
            try {
                assertEquals(HttpHeaderValues.KEEP_ALIVE.toString(),
                        response.headers().get(HttpHeaderNames.CONNECTION));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
