package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import io.netty.channel.embedded.EmbeddedChannel;
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
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
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
