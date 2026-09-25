package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HttpServerHandlerTest {

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
