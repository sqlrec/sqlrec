package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.ExecuteDataList;
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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
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
        String path = uri;

        try {
            path = new QueryStringDecoder(uri).rawPath();
            ByteBuf content = request.content();
            String postData = (content != null) ? content.toString(CharsetUtil.UTF_8) : "";

            FullHttpResponse response;
            if (HttpMethod.POST.equals(method)) {
                response = handlePost(path, postData);
            } else if (HttpMethod.GET.equals(method)) {
                response = handleGet(path, uri, method, postData);
            } else {
                response = RestUtils.error(HttpResponseStatus.METHOD_NOT_ALLOWED, "only support POST and GET methods");
            }

            writeResponse(ctx, request, response, normalizeMetricsPath(path), method, startTime);
        } catch (Exception e) {
            logger.error("Error processing request: uri={}", uri, e);
            String errorMsg = e.getMessage();
            FullHttpResponse response = RestUtils.error(HttpResponseStatus.INTERNAL_SERVER_ERROR, errorMsg != null ? errorMsg : "unknown error");
            writeResponse(ctx, request, response, normalizeMetricsPath(path), method, startTime);
        }
    }

    private FullHttpResponse handlePost(String path, String postData) throws Exception {
        if (path.equals(SQL_V1_PATH) || path.startsWith(SQL_V1_PATH + "/")) {
            return handleSql(postData);
        } else if (path.startsWith(API_V1_PREFIX)) {
            return handleApi(path, postData);
        } else {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
        }
    }

    private FullHttpResponse handleGet(String path, String uri, HttpMethod method, String postData) {
        if (path.equals(METRICS_PATH)) {
            return handleMetrics();
        } else if (path.startsWith(UI_STATIC_PREFIX) || path.startsWith(UI_API_PREFIX)) {
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

    private FullHttpResponse handleApi(String path, String postData) throws Exception {
        String apiName = path.substring(API_V1_PREFIX.length());
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

    private void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response,
                               String path, HttpMethod method, long startTime) {
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

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private String normalizeMetricsPath(String path) {
        if (path.equals(SQL_V1_PATH) || path.startsWith(SQL_V1_PATH + "/")) {
            return SQL_V1_PATH;
        } else if (path.startsWith(UI_STATIC_PREFIX)) {
            return UI_STATIC_PREFIX;
        } else {
            return path;
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleStateEvent
                && idleStateEvent.state() == IdleState.READER_IDLE) {
            logger.debug("Closing idle HTTP connection: remoteAddress={}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in HttpServerHandler", cause);
        ctx.close();
    }
}