package com.sqlrec.config;

public class ConfigOption<T> {
    public final String key;
    private final T defaultValue;
    private final String description;

    public ConfigOption(String key, T defaultValue, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getKey() {
        return key;
    }

    public Class<T> getType() {
        return (Class<T>) defaultValue.getClass();
    }
}
