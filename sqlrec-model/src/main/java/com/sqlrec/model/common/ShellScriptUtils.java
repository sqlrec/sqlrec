package com.sqlrec.model.common;

public final class ShellScriptUtils {
    private ShellScriptUtils() {
    }

    public static String quote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }
}
