package com.sqlrec.frontend;

import com.sqlrec.common.config.SqlRecConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        if (!SqlRecConfigs.ENABLE_REST_SERVER.getValue() && !SqlRecConfigs.ENABLE_THRIFT_SERVER.getValue()) {
            throw new IllegalArgumentException("ENABLE_REST_SERVER and ENABLE_THRIFT_SERVER are both false");
        }

        if (SqlRecConfigs.ENABLE_REST_SERVER.getValue()) {
            startServer(() -> {
                try {
                    RestServer.main(args);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        if (SqlRecConfigs.ENABLE_THRIFT_SERVER.getValue()) {
            startServer(() -> {
                try {
                    ThriftServer.main(args);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            logger.info("servers exited");
            System.exit(1);
        }
    }

    private static void startServer(Runnable server) {
        new Thread(() -> {
            try {
                server.run();
            } catch (Exception e) {
                logger.error("Error in server", e);
                throw new RuntimeException(e);
            } finally {
                latch.countDown();
            }
        }).start();
    }
}
