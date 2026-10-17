package com.sqlrec.frontend.utils;

import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.config.Consts;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.java21.instrument.binder.jdk.VirtualThreadMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.lang.management.ManagementFactory;

public class PrometheusMetricsUtils {
    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsUtils.class);
    private static final List<AutoCloseable> binders = new ArrayList<>();
    private static volatile PrometheusMeterRegistry prometheusRegistry;

    public static PrometheusMeterRegistry getPrometheusRegistry() {
        return prometheusRegistry;
    }

    public static synchronized void initMetrics() {
        if (prometheusRegistry != null) {
            return;
        }

        CompositeMeterRegistry meterRegistry = MetricsUtils.getCompositeMeterRegistry();
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsUtils.configureRegistry(registry);
        meterRegistry.add(registry);
        new ClassLoaderMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        var memory = ManagementFactory.getMemoryMXBean();
        Gauge.builder("sqlrec.jvm.heap.used.bytes", memory, bean -> bean.getHeapMemoryUsage().getUsed())
                .description("Total JVM heap used, without summing shared pool limits").register(meterRegistry);
        Gauge.builder("sqlrec.jvm.heap.max.bytes", memory, bean -> bean.getHeapMemoryUsage().getMax())
                .description("JVM heap maximum; -1 means undefined").register(meterRegistry);
        JvmGcMetrics gcMetrics = new JvmGcMetrics();
        gcMetrics.bindTo(meterRegistry);
        binders.add(gcMetrics);
        new ProcessorMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new JvmThreadDeadlockMetrics().bindTo(meterRegistry);
        new FileDescriptorMetrics().bindTo(meterRegistry);
        new UptimeMetrics().bindTo(meterRegistry);
        VirtualThreadMetrics virtualThreadMetrics = new VirtualThreadMetrics();
        virtualThreadMetrics.bindTo(meterRegistry);
        binders.add(virtualThreadMetrics);

        meterRegistry.counter(Consts.METRICS_REGISTRATION_FAILURES);
        prometheusRegistry = registry;
        Runtime.getRuntime().addShutdownHook(new Thread(PrometheusMetricsUtils::closeMetrics, "metrics-shutdown"));
    }

    private static synchronized void closeMetrics() {
        for (AutoCloseable binder : binders) {
            try {
                binder.close();
            } catch (Exception e) {
                log.debug("Failed to close metrics binder", e);
            }
        }
        binders.clear();
        if (prometheusRegistry != null) {
            MetricsUtils.getCompositeMeterRegistry().remove(prometheusRegistry);
            prometheusRegistry.close();
        }
    }
}
