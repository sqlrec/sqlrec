package com.sqlrec.frontend.utils;

import com.sqlrec.common.utils.JsonUtils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RestUtilsTest {

    @Test
    void errorEscapesMessageAsValidJson() {
        String message = "invalid \"value\" at C:\\data\nnext line";
        FullHttpResponse response = RestUtils.error(HttpResponseStatus.BAD_REQUEST, message);

        try {
            Map<String, Object> body = JsonUtils.parseJsonToMap(response.content().toString(CharsetUtil.UTF_8));

            assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
            assertEquals(message, body.get("msg"));
        } finally {
            response.release();
        }
    }
}
