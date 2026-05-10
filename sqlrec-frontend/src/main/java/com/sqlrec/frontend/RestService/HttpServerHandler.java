package com.sqlrec.frontend.RestService;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.common.PrometheusMetricsUtils;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);
    private static final String SQL_V1_PATH = "/sql/v1";
    private static final String API_V1_PREFIX = "/api/v1/";
    private static final String METRICS_PATH = "/metrics";

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) throws Exception {
        long startTime = System.currentTimeMillis();
        String uri = fullHttpRequest.uri();
        HttpMethod method = fullHttpRequest.method();
        String path = extractPath(uri);

        String responseContent = "{}";
        String contentType = "application/json";
        HttpResponseStatus status = HttpResponseStatus.OK;

        try {
            ByteBuf content = fullHttpRequest.content();
            String postData = (content != null) ? content.toString(CharsetUtil.UTF_8) : "";

            if (HttpMethod.POST.equals(method)) {
                if (uri.equals(SQL_V1_PATH) || uri.startsWith(SQL_V1_PATH + "/")) {
                    ExecuteDataList executeDataList = SqlExecutor.execute(postData);
                    responseContent = JsonUtils.toJson(executeDataList);
                } else if (uri.startsWith(API_V1_PREFIX)) {
                    String apiName = uri.substring(API_V1_PREFIX.length());
                    if (apiName.isEmpty()) {
                        status = HttpResponseStatus.BAD_REQUEST;
                        responseContent = "{\"msg\":\"api name is required\"}";
                    } else {
                        ExecuteData executeData = FunctionExecutor.execute(apiName, postData);
                        responseContent = JsonUtils.toJson(executeData);
                    }
                } else {
                    status = HttpResponseStatus.NOT_FOUND;
                    responseContent = "{\"msg\":\"uri not found\"}";
                }
            } else if (HttpMethod.GET.equals(method)) {
                if (uri.equals(METRICS_PATH)) {
                    responseContent = PrometheusMetricsUtils.getPrometheusRegistry().scrape();
                    status = HttpResponseStatus.OK;
                    contentType = "text/plain";
                } else {
                    status = HttpResponseStatus.NOT_FOUND;
                    responseContent = "{\"msg\":\"uri not found\"}";
                }
            } else {
                status = HttpResponseStatus.METHOD_NOT_ALLOWED;
                responseContent = "{\"msg\":\"only support POST and GET methods\"}";
            }
        } catch (Exception e) {
            logger.error("Error processing request: uri={}", uri, e);
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            Map<String, String> errorMap = new HashMap<>();
            String errorMsg = e.getMessage();
            errorMap.put("msg", errorMsg != null ? errorMsg : "unknown error");
            responseContent = JsonUtils.toJson(errorMap);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            Tags tags = Tags.of("path", path)
                    .and("method", method.name())
                    .and("status", String.valueOf(status.code()));

            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_HTTP_REQUEST_DURATION, tags)
                    .record(duration, TimeUnit.MILLISECONDS);

            MetricsUtils.getCompositeMeterRegistry()
                    .counter(Consts.METRICS_HTTP_REQUEST_COUNT, tags)
                    .increment();
        }

        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(responseContent, CharsetUtil.UTF_8)
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        channelHandlerContext.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private String extractPath(String uri) {
        if (uri.equals(SQL_V1_PATH) || uri.startsWith(SQL_V1_PATH + "/")) {
            return SQL_V1_PATH;
        } else if (uri.startsWith(API_V1_PREFIX)) {
            String apiName = uri.substring(API_V1_PREFIX.length());
            int queryIndex = apiName.indexOf('?');
            if (queryIndex > 0) {
                apiName = apiName.substring(0, queryIndex);
            }
            return API_V1_PREFIX + apiName;
        } else if (uri.equals(METRICS_PATH)) {
            return METRICS_PATH;
        } else {
            int queryIndex = uri.indexOf('?');
            if (queryIndex > 0) {
                return uri.substring(0, queryIndex);
            }
            return uri;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in HttpServerHandler", cause);
        ctx.close();
    }
}