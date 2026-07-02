package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TraceUtils {
    private static final Logger log = LoggerFactory.getLogger(TraceUtils.class);
    private static volatile OpenTelemetrySdk openTelemetry;
    private static volatile Tracer tracer;
    private static volatile boolean sdkInitialized = false;
    private static volatile boolean initFailed = false;
    private static final Object INIT_LOCK = new Object();

    private TraceUtils() {
    }

    public static void initIfNeeded() {
        if (sdkInitialized || initFailed) {
            return;
        }
        synchronized (INIT_LOCK) {
            if (sdkInitialized || initFailed) {
                return;
            }
            try {
                String endpoint = SqlRecConfigs.TRACE_ENDPOINT.getValue();
                String headersStr = SqlRecConfigs.TRACE_HEADERS.getValue();
                String serviceName = SqlRecConfigs.TRACE_SERVICE_NAME.getValue();

                var exporterBuilder = OtlpGrpcSpanExporter.builder()
                        .setEndpoint(endpoint);
                Map<String, String> headers = parseHeaders(headersStr);
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    exporterBuilder.addHeader(entry.getKey(), entry.getValue());
                }
                OtlpGrpcSpanExporter exporter = exporterBuilder.build();

                Resource resource = Resource.getDefault().merge(
                        Resource.create(Attributes.of(
                                ResourceAttributes.SERVICE_NAME, serviceName
                        ))
                );

                SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                        .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                        .setResource(resource)
                        .build();

                openTelemetry = OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .buildAndRegisterGlobal();
                tracer = openTelemetry.getTracer("sqlrec", SqlRecConfigs.SQLREC_VERSION.getValue());
                sdkInitialized = true;

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    openTelemetry.getSdkTracerProvider().close();
                }));

                log.info("OpenTelemetry trace SDK initialized, tracing enabled");
            } catch (Throwable e) {
                log.warn("Failed to initialize OpenTelemetry trace SDK, tracing disabled", e);
                initFailed = true;
            }
        }
    }

    public static boolean shouldTrace(ExecuteContext context) {
        if (StringUtils.isEmpty(SqlRecConfigs.TRACE_ENDPOINT.getValue()) || initFailed) {
            return false;
        }
        Map<String, String> vars = context.getVariables();
        if (vars != null && vars.containsKey(SqlRecConfigs.DEBUG_TRACE.getKey())) {
            return SqlRecConfigs.DEBUG_TRACE.getValue(vars);
        }
        return SqlRecConfigs.DEBUG_TRACE.getValue();
    }

    public static Span startSpan(ExecuteContext context, String spanName) {
        try {
            if (!shouldTrace(context)) {
                return null;
            }
            initIfNeeded();
            Context parentOtelContext = unwrapContext(context.getTraceContext());
            Span span = tracer.spanBuilder(spanName)
                    .setParent(parentOtelContext)
                    .startSpan();
            context.setTraceContext(parentOtelContext.with(span));
            return span;
        } catch (Throwable e) {
            log.warn("Failed to start trace span, skipping", e);
            return null;
        }
    }

    public static void endSpan(Span span, String logId, long durationMs, long dataCount,
                               String status, Throwable error) {
        if (span == null) {
            return;
        }
        try {
            span.setAttribute("log.id", logId);
            span.setAttribute("duration.ms", durationMs);
            span.setAttribute("data.count", dataCount);
            span.setAttribute("status", status);
            if (error != null) {
                span.setStatus(StatusCode.ERROR, error.getMessage());
                span.recordException(error);
            } else {
                span.setStatus(StatusCode.OK);
            }
            span.end();
        } catch (Throwable e) {
            log.warn("Failed to end trace span, skipping", e);
        }
    }

    static Context unwrapContext(Object traceContext) {
        if (traceContext instanceof Context) {
            return (Context) traceContext;
        }
        return Context.root();
    }

    private static Map<String, String> parseHeaders(String headersStr) {
        if (headersStr == null || headersStr.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new HashMap<>();
        for (String pair : headersStr.split(",")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                headers.put(kv[0].trim(), kv[1].trim());
            }
        }
        return headers;
    }
}