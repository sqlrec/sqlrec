package com.sqlrec.common.config;

import java.util.List;
import java.util.Map;

public class ConfigOption<T> {
    private final String key;
    private T defaultValue;
    private final String description;
    private final List<T> validValues;
    private final Class<T> type;

    public ConfigOption(String key, T defaultValue, String description, List<T> validValues, Class<T> type) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
        this.validValues = validValues;
        this.type = type;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getKey() {
        return key;
    }

    public Class<T> getType() {
        return type;
    }

    /** Resolves this option from the supplied map and then the declared default value. */
    public T getValue(Map<String, String> options) {
        return processValue(getOptionValue(options));
    }

    /**
     * Resolves this option from the supplied map, then the process environment, and finally the
     * declared default value.
     */
    public T getValueWithEnvFallback(Map<String, String> options) {
        String value = getOptionValue(options);
        if (value == null) {
            value = System.getenv(key);
        }
        return processValue(value);
    }

    public T getValueOrNull(Map<String, String> options) {
        String value = getOptionValue(options);
        return validateValue(getFromStr(value), value);
    }

    public boolean isSet(Map<String, String> options) {
        return getOptionValue(options) != null;
    }

    public T getValue() {
        return processValue(System.getenv(key));
    }

    private T processValue(String value) {
        T processedValue = getFromStr(value);
        if (processedValue == null) {
            throw new IllegalArgumentException(key + " is not set");
        }
        return validateValue(processedValue, value);
    }

    private T validateValue(T processedValue, String value) {
        if (validValues != null && !validValues.contains(processedValue)) {
            throw new IllegalArgumentException("Invalid value: " + value);
        }
        return processedValue;
    }

    private String getOptionValue(Map<String, String> options) {
        return options == null ? null : options.get(key);
    }

    private T getFromStr(String value) {
        if (value == null) {
            return defaultValue;
        }
        if (type == String.class) {
            return type.cast(value);
        }
        if (type == Integer.class) {
            return type.cast(Integer.valueOf(value));
        }
        if (type == Long.class) {
            return type.cast(Long.valueOf(value));
        }
        if (type == Double.class) {
            return type.cast(Double.valueOf(value));
        }
        if (type == Float.class) {
            return type.cast(Float.valueOf(value));
        }
        if (type == Short.class) {
            return type.cast(Short.valueOf(value));
        }
        if (type == Byte.class) {
            return type.cast(Byte.valueOf(value));
        }
        if (type == Boolean.class) {
            return type.cast(Boolean.valueOf(value));
        }
        throw new UnsupportedOperationException("Not supported type: " + type);
    }
}
