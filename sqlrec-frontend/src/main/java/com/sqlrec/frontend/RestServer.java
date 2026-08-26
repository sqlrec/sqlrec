package com.sqlrec.frontend;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.FunctionUpdater;
import com.sqlrec.frontend.rest.HttpServerHandler;
import com.sqlrec.frontend.utils.PrometheusMetricsUtils;
import com.sqlrec.schema.CalciteSchemaFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestServer {
    private static final Logger logger = LoggerFactory.getLogger(RestServer.class);

    public static void main(String[] args) throws InterruptedException {
        int businessExecutorThreads = SqlRecConfigs.REST_BUSINESS_EXECUTOR_THREADS.getValue();
        int businessMaxPendingTasks = SqlRecConfigs.REST_BUSINESS_MAX_PENDING_TASKS.getValue();
        validateBusinessExecutorConfig(businessExecutorThreads, businessMaxPendingTasks);

        FunctionUpdater.initFunctionUpdateService();
        PrometheusMetricsUtils.initMetrics();
        CalciteSchemaFactory.createCalciteSchema();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(
                businessExecutorThreads,
                new DefaultThreadFactory("rest-business"),
                businessMaxPendingTasks,
                RejectedExecutionHandlers.reject());

        try {
            logger.info(
                    "RestServer is running on port {}, businessExecutorThreads={}, businessMaxPendingTasks={}",
                    SqlRecConfigs.REST_SERVER_PORT.getValue(),
                    businessExecutorThreads,
                    businessMaxPendingTasks);
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(
                                    SqlRecConfigs.REST_MAX_CONTENT_LENGTH.getValue()));
                            ch.pipeline().addLast(new IdleStateHandler(
                                    SqlRecConfigs.REST_KEEP_ALIVE_IDLE_TIMEOUT.getValue(), 0, 0));
                            ch.pipeline().addLast(businessGroup, new HttpServerHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(SqlRecConfigs.REST_SERVER_PORT.getValue()).sync();
            f.channel().closeFuture().sync();
        } finally {
            businessGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private static void validateBusinessExecutorConfig(int threads, int maxPendingTasks) {
        if (threads <= 0) {
            throw new IllegalArgumentException("REST_BUSINESS_EXECUTOR_THREADS must be greater than 0");
        }
        if (maxPendingTasks <= 0) {
            throw new IllegalArgumentException("REST_BUSINESS_MAX_PENDING_TASKS must be greater than 0");
        }
    }
}
