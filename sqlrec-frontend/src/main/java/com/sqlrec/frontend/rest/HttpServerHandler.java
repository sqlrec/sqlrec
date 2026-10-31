package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.rest.ExecuteDataList;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.frontend.auth.Authentication;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import com.sqlrec.frontend.utils.RestUtils;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelFuture;
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
        // Netty has already dispatched this request to the business executor.
        long startNanos = System.nanoTime();
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        FullHttpResponse response;

        try {
            boolean publicMetrics = HttpMethod.GET.equals(request.method())
                    && METRICS_PATH.equals(decoder.rawPath());
            if (!publicMetrics && !isAuthenticated(request)) {
                response = RestUtils.error(HttpResponseStatus.UNAUTHORIZED, "authentication required");
                response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE,
                        "Basic realm=\"SQLRec\", charset=\"UTF-8\"");
            } else if (Authentication.isEnabled() && HttpMethod.POST.equals(request.method())
                    && (SQL_V1_PATH.equals(decoder.rawPath()) || decoder.rawPath().startsWith(API_V1_PREFIX))
                    && !isJsonRequest(request)) {
                // Browsers cache Basic credentials. Require JSON so cross-origin HTML forms
                // cannot submit authenticated SQL/API requests with a safelisted content type.
                response = RestUtils.error(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                        "Content-Type must be application/json");
            } else {
                response = dispatch(parseRequest(request, decoder));
            }
        } catch (IllegalArgumentException e) {
            logger.debug("Invalid HTTP request: uri={}, message={}", request.uri(), e.getMessage());
            response = RestUtils.error(HttpResponseStatus.BAD_REQUEST, RestUtils.errorMessage(e, "invalid request"));
        } catch (Exception e) {
            logger.error("Error processing HTTP request: uri={}", request.uri(), e);
            response = RestUtils.error(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    RestUtils.errorMessage(e, "internal server error"));
        }

        writeResponse(ctx, request, response, decoder.rawPath(), startNanos);
    }

    private boolean isAuthenticated(FullHttpRequest request) {
        if (!Authentication.isEnabled()) {
            return true;
        }
        List<String> headers = request.headers().getAll(HttpHeaderNames.AUTHORIZATION);
        return headers.size() == 1 && Authentication.authenticateBasic(headers.get(0));
    }

    private boolean isJsonRequest(FullHttpRequest request) {
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        return contentType != null && "application/json".equalsIgnoreCase(contentType.split(";", 2)[0].trim());
    }

    private RequestContext parseRequest(FullHttpRequest request, QueryStringDecoder decoder) {
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
        return RestUtils.error(HttpResponseStatus.NOT_FOUND, "path not found: " + path);
    }

    private FullHttpResponse requireMethod(RequestContext request, HttpMethod allowedMethod,
                                           RequestHandler handler) throws Exception {
        if (allowedMethod.equals(request.method())) {
            return handler.handle();
        }

        FullHttpResponse response = RestUtils.error(
                HttpResponseStatus.METHOD_NOT_ALLOWED,
                "method " + request.method() + " is not allowed for " + request.path()
                        + "; use " + allowedMethod);
        response.headers().set(HttpHeaderNames.ALLOW, allowedMethod.name());
        return response;
    }

    // Endpoints

    private FullHttpResponse handleSql(String requestBody) throws Exception {
        if (!SqlRecConfigs.ENABLE_REST_SQL_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN,
                    "SQL API is disabled (ENABLE_REST_SQL_API=false)");
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
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "path not found: " + path);
        }

        String apiName = QueryStringDecoder.decodeComponent(rawApiName);
        if (apiName.contains("/")) {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "path not found: " + path);
        }
        ExecuteData result = RestFunctionExecutor.execute(apiName, requestBody);
        return RestUtils.ok(JsonUtils.toJson(result));
    }

    private FullHttpResponse handleMetrics() {
        return RestUtils.ok(PrometheusMetricsUtils.getPrometheusRegistry().scrape(), "text/plain");
    }

    private FullHttpResponse handleUi(RequestContext request) throws Exception {
        if (!SqlRecConfigs.ENABLE_REST_UI_API.getValue()) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN,
                    "UI API is disabled (ENABLE_REST_UI_API=false)");
        }
        return uiHandler.handleRequest(request.path(), request.queryParameters());
    }

    // Response and metrics

    private void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request,
                               FullHttpResponse response, String path, long startNanos) {
        Tags tags = Tags.of("path", metricsPath(path))
                .and("method", request.method().name())
                .and("status", String.valueOf(response.status().code()));

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        response.headers().set(HttpHeaderNames.CONNECTION,
                keepAlive ? HttpHeaderValues.KEEP_ALIVE : HttpHeaderValues.CLOSE);
        // Notify on the I/O loop so later business tasks do not delay this measurement.
        ChannelFuture written = ctx.writeAndFlush(response, ctx.channel().newPromise());
        written.addListener(future -> MetricsUtils.getCompositeMeterRegistry()
                .timer(Consts.METRICS_HTTP_REQUEST_DURATION, tags)
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));
        if (!keepAlive) {
            written.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static String metricsPath(String path) {
        if (SQL_V1_PATH.equals(path) || METRICS_PATH.equals(path)) {
            return path;
        }
        if (path.startsWith(API_V1_PREFIX)) {
            return path;
        }
        if (path.startsWith(UI_STATIC_PREFIX)) {
            return UI_STATIC_PREFIX + "*";
        }
        if (path.startsWith(UI_API_PREFIX)) {
            return UI_API_PREFIX + "*";
        }
        return "unmatched";
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
