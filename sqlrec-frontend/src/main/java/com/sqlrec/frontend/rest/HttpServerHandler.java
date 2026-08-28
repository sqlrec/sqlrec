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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);
    private static final UiHandler DEFAULT_UI_HANDLER = new UiHandler();

    public static final String SQL_V1_PATH = "/sql/v1";
    public static final String API_V1_PREFIX = "/api/v1/";
    public static final String METRICS_PATH = "/metrics";
    public static final String UI_STATIC_PREFIX = "/ui/static/";
    public static final String UI_API_PREFIX = "/ui/api/";

    private final UiHandler uiHandler;

    public HttpServerHandler() {
        this(DEFAULT_UI_HANDLER);
    }

    HttpServerHandler(UiHandler uiHandler) {
        this.uiHandler = uiHandler;
    }

    // Request lifecycle

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        long startNanos = System.nanoTime();
        RequestContext requestContext = null;
        FullHttpResponse response;

        try {
            requestContext = parseRequest(request);
            response = dispatch(requestContext);
        } catch (IllegalArgumentException e) {
            logger.debug("Invalid HTTP request: uri={}, message={}", request.uri(), e.getMessage());
            response = RestUtils.error(HttpResponseStatus.BAD_REQUEST, errorMessage(e, "invalid request"));
        } catch (Exception e) {
            logger.error("Error processing HTTP request: uri={}", request.uri(), e);
            response = RestUtils.error(HttpResponseStatus.INTERNAL_SERVER_ERROR, "internal server error");
        }

        String metricsPath = requestContext == null
                ? "unmatched"
                : normalizeMetricsPath(requestContext.path());
        writeResponse(ctx, request, response, metricsPath, startNanos);
    }

    private RequestContext parseRequest(FullHttpRequest request) {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        return new RequestContext(
                request.method(),
                decoder.rawPath(),
                decoder.parameters(),
                request.content().toString(CharsetUtil.UTF_8));
    }

    // Routing

    private FullHttpResponse dispatch(RequestContext request) throws Exception {
        String path = request.path();

        if (SQL_V1_PATH.equals(path)) {
            return requireMethod(request, HttpMethod.POST, () -> handleSql(request.body()));
        }
        if (path.startsWith(API_V1_PREFIX)) {
            return requireMethod(request, HttpMethod.POST, () -> handleApi(path, request.body()));
        }
        if (METRICS_PATH.equals(path)) {
            return requireMethod(request, HttpMethod.GET, this::handleMetrics);
        }
        if (path.startsWith(UI_STATIC_PREFIX) || path.startsWith(UI_API_PREFIX)) {
            return requireMethod(request, HttpMethod.GET, () -> handleUi(request));
        }
        return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
    }

    private FullHttpResponse requireMethod(RequestContext request, HttpMethod allowedMethod,
                                           RequestHandler handler) throws Exception {
        if (allowedMethod.equals(request.method())) {
            return handler.handle();
        }

        FullHttpResponse response = RestUtils.error(
                HttpResponseStatus.METHOD_NOT_ALLOWED,
                "method not allowed");
        response.headers().set(HttpHeaderNames.ALLOW, allowedMethod.name());
        return response;
    }

    // Endpoints

    private FullHttpResponse handleSql(String requestBody) throws Exception {
        if (!SqlRecConfigs.ENABLE_REST_SQL_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN, "sql api is disabled");
        }
        ExecuteDataList result = RestSqlExecutor.execute(requestBody);
        return RestUtils.ok(JsonUtils.toJson(result));
    }

    private FullHttpResponse handleApi(String path, String requestBody) throws Exception {
        String rawApiName = path.substring(API_V1_PREFIX.length());
        if (rawApiName.isEmpty()) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST, "api name is required");
        }
        if (rawApiName.contains("/")) {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
        }

        String apiName = QueryStringDecoder.decodeComponent(rawApiName);
        if (apiName.contains("/")) {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "uri not found");
        }
        ExecuteData result = RestFunctionExecutor.execute(apiName, requestBody);
        return RestUtils.ok(JsonUtils.toJson(result));
    }

    private FullHttpResponse handleMetrics() {
        return RestUtils.ok(PrometheusMetricsUtils.getPrometheusRegistry().scrape(), "text/plain");
    }

    private FullHttpResponse handleUi(RequestContext request) throws Exception {
        if (!SqlRecConfigs.ENABLE_REST_UI_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN, "ui api is disabled");
        }
        return uiHandler.handleRequest(request.path(), request.queryParameters());
    }

    // Response and metrics

    private void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request,
                               FullHttpResponse response, String path, long startNanos) {
        long durationNanos = System.nanoTime() - startNanos;
        Tags tags = Tags.of("path", path)
                .and("method", request.method().name())
                .and("status", String.valueOf(response.status().code()));

        MetricsUtils.getCompositeMeterRegistry()
                .timer(Consts.METRICS_HTTP_REQUEST_DURATION, tags)
                .record(durationNanos, TimeUnit.NANOSECONDS);
        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_HTTP_REQUEST_COUNT, tags)
                .increment();

        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private String normalizeMetricsPath(String path) {
        if (SQL_V1_PATH.equals(path)) {
            return SQL_V1_PATH;
        }
        if (path.startsWith(API_V1_PREFIX)) {
            return API_V1_PREFIX + "{apiName}";
        }
        if (METRICS_PATH.equals(path)) {
            return METRICS_PATH;
        }
        if (path.startsWith(UI_STATIC_PREFIX)) {
            return UI_STATIC_PREFIX + "{resource}";
        }
        if (path.startsWith(UI_API_PREFIX)) {
            return UI_API_PREFIX + "{resource}";
        }
        return "unmatched";
    }

    private String errorMessage(Exception e, String fallback) {
        return e.getMessage() == null || e.getMessage().isBlank() ? fallback : e.getMessage();
    }

    // Connection lifecycle

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

    private record RequestContext(
            HttpMethod method,
            String path,
            Map<String, List<String>> queryParameters,
            String body) {
    }

    @FunctionalInterface
    private interface RequestHandler {
        FullHttpResponse handle() throws Exception;
    }
}