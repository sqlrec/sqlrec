package com.sqlrec.frontend.RestService;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) throws Exception {
        String uri = fullHttpRequest.uri();
        HttpMethod method = fullHttpRequest.method();

        String responseContent = "{}";
        HttpResponseStatus status = HttpResponseStatus.OK;

        if (!HttpMethod.POST.equals(method)) {
            status = HttpResponseStatus.METHOD_NOT_ALLOWED;
            responseContent = "{\"msg\":\"only support post method\"}";
        } else {
            String postData = fullHttpRequest.content().toString(CharsetUtil.UTF_8);
            String apiName = "";
            if (!uri.startsWith("/api/v1/")) {
                status = HttpResponseStatus.METHOD_NOT_ALLOWED;
                responseContent = "{\"msg\":\"uri format error\"}";
            } else {
                apiName = uri.substring("/api/v1/".length());
                try {
                    responseContent = FunctionExecutor.execute(apiName, postData);
                } catch (Exception e) {
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                    responseContent = "{\"msg\":\"" + e.getMessage() + "\"}";
                }
            }
        }

        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(responseContent, CharsetUtil.UTF_8)
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        channelHandlerContext.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
