package com.sqlrec.frontend.common;

import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.java21.instrument.binder.jdk.VirtualThreadMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

public class PrometheusMetricsUtils {
    private static PrometheusMeterRegistry prometheusRegistry;

    public static PrometheusMeterRegistry getPrometheusRegistry() {
        return prometheusRegistry;
    }

    public static synchronized void initMetrics() {
        if (prometheusRegistry != null) {
            return;
        }

        CompositeMeterRegistry meterRegistry = MetricsUtils.getCompositeMeterRegistry();
        new ClassLoaderMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new JvmThreadDeadlockMetrics().bindTo(meterRegistry);
        new VirtualThreadMetrics().bindTo(meterRegistry);

        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        meterRegistry.add(prometheusRegistry);
    }

}
