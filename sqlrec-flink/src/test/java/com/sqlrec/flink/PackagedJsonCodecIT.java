package com.sqlrec.flink;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PackagedJsonCodecIT {
    @Test
    void jsonLookupDecodesWithoutAccessToPlannerInternals() throws Exception {
        URL jar = Path.of(System.getProperty("sqlrec.shadedJar")).toUri().toURL();
        // No fallback to the test JVM's Calcite or visible planner dependencies.
        try (URLClassLoader loader = new URLClassLoader(new URL[]{jar}, ClassLoader.getPlatformClassLoader())) {
            var field = loader.loadClass("com.sqlrec.common.schema.FieldSchema")
                    .getConstructor(String.class, String.class);
            Class<?> codecType = loader.loadClass("com.sqlrec.connectors.redis.codec.JsonCodec");
            Object codec = codecType.getConstructor().newInstance();
            codecType.getMethod("init", List.class, int.class).invoke(codec,
                    List.of(field.newInstance("id", "INTEGER"), field.newInstance("scores", "ARRAY<FLOAT>"),
                            field.newInstance("names", "ARRAY<STRING>")), 0);
            byte[] json = "{\"id\":7e0,\"scores\":[0.25,null],\"names\":[\"first\",2]}"
                    .getBytes(StandardCharsets.UTF_8);
            Object[] row = (Object[]) codecType.getMethod("decode", byte[].class, String.class)
                    .invoke(codec, json, "7");
            assertArrayEquals(new Object[]{7, Arrays.asList(0.25d, null), List.of("first", "2")}, row);
        }
    }
}
