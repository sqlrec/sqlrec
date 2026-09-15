package com.sqlrec.model.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShellScriptUtilsTest {

    @Test
    void quotesSingleQuotesForShellArguments() {
        assertEquals("'model'\\''s path'", ShellScriptUtils.quote("model's path"));
    }
}
