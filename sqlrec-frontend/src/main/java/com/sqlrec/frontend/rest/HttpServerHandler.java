package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import com.sqlrec.frontend.utils.RestUtils;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);
    public static final String SQL_V1_PATH = "/sql/v1";
    public static final String API_V1_PREFIX = "/api/v1/";
    public static final String METRICS_PATH = "/metrics";
    public static final String UI_STATIC_PREFIX = "/ui/static/";
    public static final String UI_API_PREFIX = "/ui/api/";

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        long startTime = System.currentTimeMillis();
        String uri = request.uri();
        HttpMethod method = request.method();
        String path = extractPath(uri);

        try {
            ByteBuf content = request.content();
            String postData = (content != null) ? content.toString(CharsetUtil.UTF_8) : "";

            FullHttpResponse response;
            if (HttpMethod.POST.equals(method)) {
                response = handlePost(uri, postData);
            } else if (HttpMethod.GET.equals(method)) {
                response = handleGet(uri, method, postData);
            } else {
                response = RestUtils.error(HttpResponseStatus.METHOD_NOT_ALLOWED, "only support POST and GET methods");
            }

            writeResponse(ctx, response, path, method, startTime);
        } catch (Exception e) {
            logger.error("Error processing request: uri={}", uri, e);
            String errorMsg = e.getMessage();
            FullHttpResponse response = RestUtils.error(HttpResponseStatus.INTERNAL_SERVER_ERROR, errorMsg != null ? errorMsg : "unknown error");
            writeResponse(ctx, response, path, method, startTime);
        }
    }

    private FullHttpResponse handlePost(String uri, String postData) throws Exception {
        if (uri.equals(SQL_V1_PATH) || uri.startsWith(SQL_V1_PATH + "/")) {
            return handleSql(postData);
        } else if (uri.startsWith(API_V1_PREFIX)) {
            return handleApi(uri, postData);
        } else {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
        }
    }

    private FullHttpResponse handleGet(String uri, HttpMethod method, String postData) {
        if (uri.equals(METRICS_PATH)) {
            return handleMetrics();
        } else if (uri.startsWith(UI_STATIC_PREFIX) || uri.startsWith(UI_API_PREFIX)) {
            return handleUi(uri, method, postData);
        } else {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
        }
    }

    private FullHttpResponse handleSql(String postData) throws Exception {
        if (!SqlRecConfigs.ENABLE_REST_SQL_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN, "sql api is disabled");
        }
        ExecuteDataList executeDataList = RestSqlExecutor.execute(postData);
        return RestUtils.ok(JsonUtils.toJson(executeDataList));
    }

    private FullHttpResponse handleApi(String uri, String postData) throws Exception {
        String apiName = uri.substring(API_V1_PREFIX.length());
        if (apiName.isEmpty()) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST, "api name is required");
        }
        ExecuteData executeData = RestFunctionExecutor.execute(apiName, postData);
        return RestUtils.ok(JsonUtils.toJson(executeData));
    }

    private FullHttpResponse handleMetrics() {
        return RestUtils.ok(PrometheusMetricsUtils.getPrometheusRegistry().scrape(), "text/plain");
    }

    private FullHttpResponse handleUi(String uri, HttpMethod method, String postData) {
        if (!SqlRecConfigs.ENABLE_REST_UI_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN, "ui api is disabled");
        }
        return UiHandler.handleRequest(uri, method, postData);
    }

    private void writeResponse(ChannelHandlerContext ctx, FullHttpResponse response, String path, HttpMethod method, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        Tags tags = Tags.of("path", path)
                .and("method", method.name())
                .and("status", String.valueOf(response.status().code()));

        MetricsUtils.getCompositeMeterRegistry()
                .timer(Consts.METRICS_HTTP_REQUEST_DURATION, tags)
                .record(duration, TimeUnit.MILLISECONDS);

        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_HTTP_REQUEST_COUNT, tags)
                .increment();

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private String extractPath(String uri) {
        int queryIndex = uri.indexOf('?');
        String path = (queryIndex > 0) ? uri.substring(0, queryIndex) : uri;

        if (path.equals(SQL_V1_PATH) || path.startsWith(SQL_V1_PATH + "/")) {
            return SQL_V1_PATH;
        } else if (path.startsWith(UI_STATIC_PREFIX)) {
            return UI_STATIC_PREFIX;
        } else {
            return path;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in HttpServerHandler", cause);
        ctx.close();
    }
}