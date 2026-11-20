package com.sqlrec.frontend;

import com.sqlrec.utils.PathUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeDependencyTest {

    @Test
    void resolvesPathUsingHadoopConfigurationFromFrontendRuntimeClasspath() {
        String path = "/user/sqlrec/models/runtime-dependency-test";

        List<String> resolvedPaths = assertDoesNotThrow(
                () -> PathUtils.fixPathProtocol(Collections.singletonList(path)));

        assertEquals(1, resolvedPaths.size());
        assertTrue(resolvedPaths.get(0).endsWith(path));
    }
}
