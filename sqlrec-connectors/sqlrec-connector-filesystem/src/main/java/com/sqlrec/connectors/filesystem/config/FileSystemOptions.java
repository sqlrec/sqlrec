package com.sqlrec.connectors.filesystem.config;

import com.sqlrec.common.config.ConfigOption;

import java.util.Arrays;
import java.util.Map;

public class FileSystemOptions {
    public static final String CONNECTOR_IDENTIFIER = "filesystem";

    public static final ConfigOption<String> PATH = new ConfigOption<>(
            "path",
            null,
            "File system path, e.g. file:///path/to/data",
            null,
            String.class
    );

    public static final ConfigOption<String> FORMAT = new ConfigOption<>(
            "format",
            "csv",
            "File format: csv or json",
            Arrays.asList("csv", "json"),
            String.class
    );

    public static FileSystemConfig getFileSystemConfig(Map<String, String> options) {
        FileSystemConfig config = new FileSystemConfig();
        config.path = options != null && options.containsKey(PATH.getKey())
                ? PATH.getValue(options) : null;
        config.format = FORMAT.getValue(options);
        return config;
    }
}
