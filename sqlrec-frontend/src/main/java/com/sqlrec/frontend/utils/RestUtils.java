package com.sqlrec.frontend.utils;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.RequestData;
import com.sqlrec.common.utils.JsonUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RestUtils {
    private static final Gson FUNCTION_RESPONSE_JSON = createFunctionResponseGson();

    public static FullHttpResponse ok(String content) {
        return ok(content, "application/json");
    }

    public static FullHttpResponse okFunctionResult(ExecuteData result) {
        return ok(FUNCTION_RESPONSE_JSON.toJson(result));
    }

    public static FullHttpResponse ok(String content, String contentType) {
        return build(HttpResponseStatus.OK, content.getBytes(StandardCharsets.UTF_8), contentType, null);
    }

    public static FullHttpResponse ok(byte[] content, String contentType, Map<String, String> extraHeaders) {
        return build(HttpResponseStatus.OK, content, contentType, extraHeaders);
    }

    public static FullHttpResponse error(HttpResponseStatus status, String msg) {
        String content = JsonUtils.toJson(Map.of("msg", String.valueOf(msg)));
        return build(status, content.getBytes(StandardCharsets.UTF_8), "application/json", null);
    }

    public static RequestData parseRequestData(
            String requestBody, String missingBodyMessage, String nonObjectBodyMessage) {
        if (StringUtils.isBlank(requestBody)) {
            throw new IllegalArgumentException(missingBodyMessage);
        }
        RequestData request;
        try {
            request = JsonUtils.fromJson(requestBody, RequestData.class);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("invalid JSON request body: "
                    + errorMessage(e, "could not parse JSON"), e);
        }
        if (request == null) {
            throw new IllegalArgumentException(nonObjectBodyMessage);
        }
        return request;
    }

    public static String errorMessage(Exception exception, String fallback) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? fallback : message;
    }

    private static Gson createFunctionResponseGson() {
        Gson gson = JsonUtils.getGson();
        TypeToken<Map<String, Object>> rowType = new TypeToken<>() {};
        // Keep nulls in result rows without changing the response envelope or nested values.
        TypeAdapter<Map<String, Object>> rowAdapter = new TypeAdapter<>() {
            @Override
            public void write(JsonWriter writer, Map<String, Object> row) throws IOException {
                writer.beginObject();
                for (Map.Entry<String, Object> cell : row.entrySet()) {
                    writer.name(cell.getKey());
                    Object value = cell.getValue();
                    if (value == null) {
                        writer.jsonValue("null");
                    } else {
                        gson.toJson(value, value.getClass(), writer);
                    }
                }
                writer.endObject();
            }

            @Override
            public Map<String, Object> read(JsonReader reader) throws IOException {
                return gson.getAdapter(rowType).read(reader);
            }
        };
        return gson.newBuilder().registerTypeAdapter(rowType.getType(), rowAdapter.nullSafe()).create();
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
