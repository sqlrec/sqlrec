package com.sqlrec.common.utils;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class MetricsUtils {
    private static final CompositeMeterRegistry composite = new CompositeMeterRegistry();

    public static CompositeMeterRegistry getCompositeMeterRegistry() {
        return composite;
    }

    public static Tags createTags(Map<String, String> tags, String... keyValues) {
        TreeMap<String, String> tagMap = new TreeMap<>(tags);
        if (keyValues != null) {
            if (keyValues.length % 2 == 1) {
                throw new IllegalArgumentException("size must be even, it is a set of key=value pairs");
            }
            for (int i = 0; i < keyValues.length; i += 2) {
                tagMap.put(keyValues[i], keyValues[i + 1]);
            }
        }
        return Tags.of(tagMap.entrySet().stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                .toArray(String[]::new));
    }
}
