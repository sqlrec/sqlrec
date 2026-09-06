package com.sqlrec.common.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** Temporarily disables selected loggers for tests that deliberately exercise failure paths. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(SilenceLoggers.Extension.class)
public @interface SilenceLoggers {

    Class<?>[] value();

    class Extension implements BeforeEachCallback, AfterEachCallback {
        private static final ReentrantLock LOGGER_CONFIGURATION_LOCK = new ReentrantLock();
        private static final ExtensionContext.Namespace NAMESPACE =
                ExtensionContext.Namespace.create(SilenceLoggers.class);
        private static final String STATE_KEY = "logger-states";

        @Override
        public void beforeEach(ExtensionContext context) {
            SilenceLoggers annotation = context.getRequiredTestMethod().getAnnotation(SilenceLoggers.class);
            if (annotation == null) {
                return;
            }

            LOGGER_CONFIGURATION_LOCK.lock();
            List<LoggerState> states = new ArrayList<>();
            try {
                for (Class<?> loggerClass : annotation.value()) {
                    states.add(LoggerState.disable(loggerClass.getName()));
                }
                context.getStore(NAMESPACE).put(STATE_KEY, states);
            } catch (RuntimeException | Error e) {
                restore(states);
                LOGGER_CONFIGURATION_LOCK.unlock();
                throw e;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void afterEach(ExtensionContext context) {
            List<LoggerState> states = context.getStore(NAMESPACE).remove(STATE_KEY, List.class);
            if (states == null) {
                return;
            }
            try {
                restore(states);
            } finally {
                LOGGER_CONFIGURATION_LOCK.unlock();
            }
        }

        private static void restore(List<LoggerState> states) {
            Collections.reverse(states);
            for (LoggerState state : states) {
                state.restore();
            }
        }
    }

    class LoggerState {
        private final String name;
        private final LoggerContext log4jContext;
        private final Configuration log4jConfiguration;
        private final Level previousLog4jLevel;
        private final boolean hadExplicitLog4jConfiguration;
        private final java.util.logging.Logger julLogger;
        private final java.util.logging.Level previousJulLevel;

        private LoggerState(String name) {
            this.name = name;
            this.log4jContext = (LoggerContext) LogManager.getContext(false);
            this.log4jConfiguration = log4jContext.getConfiguration();
            this.hadExplicitLog4jConfiguration = log4jConfiguration.getLoggers().containsKey(name);
            this.previousLog4jLevel = log4jConfiguration.getLoggerConfig(name).getLevel();
            Configurator.setLevel(name, Level.OFF);

            this.julLogger = java.util.logging.Logger.getLogger(name);
            this.previousJulLevel = julLogger.getLevel();
            julLogger.setLevel(java.util.logging.Level.OFF);
        }

        static LoggerState disable(String name) {
            return new LoggerState(name);
        }

        void restore() {
            julLogger.setLevel(previousJulLevel);
            if (hadExplicitLog4jConfiguration) {
                Configurator.setLevel(name, previousLog4jLevel);
            } else {
                log4jConfiguration.removeLogger(name);
                log4jContext.updateLoggers();
            }
        }
    }
}
