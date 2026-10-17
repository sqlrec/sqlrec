package com.sqlrec.common.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Process-wide metrics policy; request variables supply values, never the tag schema. */
public final class MetricsUtils {
    private static final Logger log = LoggerFactory.getLogger(MetricsUtils.class);
    private static final List<String> BUSINESS_KEYS = parseKeys(SqlRecConfigs.METRICS_TAG_KEYS.getValue());
    private static final Set<String> HISTOGRAM_NODES = new HashSet<>(
            parseList(SqlRecConfigs.METRICS_HISTOGRAM_NODES.getValue()));
    private static final AtomicBoolean registrationWarningLogged = new AtomicBoolean();
    private static final CompositeMeterRegistry composite = new CompositeMeterRegistry();

    private MetricsUtils() {
    }

    public static CompositeMeterRegistry getCompositeMeterRegistry() {
        return composite;
    }

    /** Bind the current table cache, replacing meters for any previous instance. */
    public static synchronized void registerTableCacheMetrics(String table, Cache<?, ?> cache) {
        // Re-registering the same meter IDs alone would keep observing the old cache.
        unregisterTableCacheMetrics(table);
        if (cache != null) {
            CaffeineCacheMetrics.monitor(composite, cache, table, "table", table);
        }
    }

    public static synchronized void unregisterTableCacheMetrics(String table) {
        composite.getMeters().stream()
                .filter(meter -> meter.getId().getName().startsWith("cache.")
                        && table.equals(meter.getId().getTag("cache"))
                        && table.equals(meter.getId().getTag("table")))
                .forEach(composite::remove);
    }

    /** Configure an exporting registry before adding it to the composite or registering meters. */
    public static void configureRegistry(MeterRegistry registry) {
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                String name = id.getName();
                boolean histogram = name.equals(Consts.METRICS_HTTP_REQUEST_DURATION)
                        || name.equals(Consts.METRICS_API_EXECUTION_DURATION)
                        || name.equals(Consts.METRICS_TABLE_SCAN_DURATION)
                        || name.equals(Consts.METRICS_TABLE_GET_BY_PRIMARY_KEY_DURATION)
                        || name.equals(Consts.METRICS_TABLE_VECTOR_SEARCH_DURATION)
                        || (name.equals(Consts.METRICS_NODE_EXEC_DURATION)
                            && HISTOGRAM_NODES.contains(id.getTag("name")));
                if (!histogram || id.getType() != Meter.Type.TIMER) {
                    return config;
                }
                // Fixed SLO buckets, not the much larger generated percentile histogram.
                double[] boundaries = {5, 10, 25, 50, 100, 250, 500, 1000, 3000, 10_000, 30_000, 60_000};
                for (int i = 0; i < boundaries.length; i++) {
                    boundaries[i] *= TimeUnit.MILLISECONDS.toNanos(1);
                }
                return DistributionStatisticConfig.builder()
                        .serviceLevelObjectives(boundaries).build().merge(config);
            }
        });
        registry.config().onMeterRegistrationFailed((id, reason) -> {
            composite.counter(Consts.METRICS_REGISTRATION_FAILURES).increment();
            if (registrationWarningLogged.compareAndSet(false, true)) {
                log.warn("Metric registration failed for {}: {}; further failures are counted", id.getName(), reason);
            }
        });
    }

    /** Snapshot business variables once per event; fixed system tags take precedence on conflicts. */
    public static Tags createTags(Map<String, String> variables, String... keyValues) {
        Tags tags = Tags.empty();
        for (String key : BUSINESS_KEYS) {
            String value = variables == null ? null : variables.get(key);
            tags = tags.and(key, value == null || value.isBlank() ? "" : value);
        }
        return tags.and(keyValues);
    }

    private static List<String> parseKeys(String configured) {
        List<String> keys = parseList(configured);
        for (String key : keys) {
            if (!key.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid business metric variable: " + key);
            }
        }
        return Collections.unmodifiableList(keys);
    }

    private static List<String> parseList(String configured) {
        Set<String> values = new LinkedHashSet<>();
        for (String value : configured.split(",")) {
            if (!value.trim().isEmpty()) {
                values.add(value.trim());
            }
        }
        return new ArrayList<>(values);
    }

    /** Classify wrapped failures without assigning arbitrary exception names to labels. */
    public static String outcome(Throwable failure, boolean cancelled) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 32; depth++, cause = cause.getCause()) {
            if (cause instanceof java.util.concurrent.TimeoutException
                    || cause instanceof java.net.SocketTimeoutException
                    || cause instanceof java.net.http.HttpTimeoutException) {
                return "timeout";
            }
            if (cause instanceof Error) {
                return "error";
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return cancelled ? "cancelled" : "error";
    }
}
