package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
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
}
