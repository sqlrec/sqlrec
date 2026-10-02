package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

public class SessionTimeoutChecker {
    private static final Logger logger = LoggerFactory.getLogger(SessionTimeoutChecker.class);

    private final ScheduledExecutorService timeoutChecker = Executors.newScheduledThreadPool(1);
    private final LongConsumer expirationHandler;

    public SessionTimeoutChecker(LongConsumer expirationHandler) {
        this.expirationHandler = expirationHandler;
    }

    public void start() {
        long checkInterval = SqlRecConfigs.SESSION_CHECK_INTERVAL.getValue();
        long sessionTimeout = SqlRecConfigs.SESSION_IDLE_TIMEOUT.getValue();

        logger.info("Starting session timeout checker, checkInterval: {}ms, sessionTimeout: {}ms",
                checkInterval, sessionTimeout);
        if (checkInterval <= 0) {
            logger.info("Session timeout checker is disabled (check interval <= 0)");
            return;
        }
        if (sessionTimeout <= 0) {
            logger.info("Session timeout checker is disabled (session timeout <= 0)");
            return;
        }

        timeoutChecker.scheduleAtFixedRate(() -> {
            try {
                expirationHandler.accept(sessionTimeout);
            } catch (Exception e) {
                logger.error("Error in timeout checker", e);
            }
        }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        logger.info("Stopping session timeout checker");
        timeoutChecker.shutdown();
        try {
            if (!timeoutChecker.awaitTermination(5, TimeUnit.SECONDS)) {
                timeoutChecker.shutdownNow();
            }
        } catch (InterruptedException e) {
            timeoutChecker.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
