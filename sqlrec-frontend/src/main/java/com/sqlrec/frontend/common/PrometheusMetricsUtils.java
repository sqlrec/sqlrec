package com.sqlrec.frontend.common;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.java21.instrument.binder.jdk.VirtualThreadMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.commons.lang3.StringUtils;

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
        String metricsPrefix = SqlRecConfigs.METRICS_PREFIX.getValue();
        if (StringUtils.isNotBlank(metricsPrefix)) {
            prometheusRegistry.config().meterFilter(new MeterFilter() {
                @Override
                public Meter.Id map(Meter.Id id) {
                    return id.withName(metricsPrefix + id.getName());
                }
            });
        }
        meterRegistry.add(prometheusRegistry);
    }

}
