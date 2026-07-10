package com.sqlrec.frontend.utils;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RestUtils {

    public static FullHttpResponse ok(String content) {
        return ok(content, "application/json");
    }

    public static FullHttpResponse ok(String content, String contentType) {
        return build(HttpResponseStatus.OK, content.getBytes(StandardCharsets.UTF_8), contentType, null);
    }

    public static FullHttpResponse ok(byte[] content, String contentType) {
        return build(HttpResponseStatus.OK, content, contentType, null);
    }

    public static FullHttpResponse ok(byte[] content, String contentType, Map<String, String> extraHeaders) {
        return build(HttpResponseStatus.OK, content, contentType, extraHeaders);
    }

    public static FullHttpResponse error(HttpResponseStatus status, String msg) {
        return build(status, ("{\"msg\":\"" + msg + "\"}").getBytes(StandardCharsets.UTF_8), "application/json", null);
    }

    public static FullHttpResponse error(HttpResponseStatus status, byte[] content, String contentType) {
        return build(status, content, contentType, null);
    }

    private static FullHttpResponse build(HttpResponseStatus status, byte[] content, String contentType, Map<String, String> extraHeaders) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(content)
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);
        if (extraHeaders != null) {
            for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
                response.headers().set(entry.getKey(), entry.getValue());
            }
        }
        return response;
    }
}
