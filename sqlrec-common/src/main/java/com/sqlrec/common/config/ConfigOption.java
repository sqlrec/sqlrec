package com.sqlrec.common.config;

import java.util.List;
import java.util.Map;

public class ConfigOption<T> {
    public final String key;
    private final T defaultValue;
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

    public String getDescription() {
        return description;
    }

    public String getKey() {
        return key;
    }

    public Class<T> getType() {
        return type;
    }

    public T getValue(Map<String, String> options) {
        String value = options.get(key);
        return processValue(value);
    }

    public T getValue() {
        //get from environment variable
        String value = System.getenv(key);
        return processValue(value);
    }

    private T processValue(String value) {
        T processedValue = getFromStr(value);
        if (processedValue == null) {
            throw new IllegalArgumentException(key + " is not set");
        }
        if (validValues != null && !validValues.contains(processedValue)) {
            throw new IllegalArgumentException("Invalid value: " + value);
        }
        return processedValue;
    }

    private T getFromStr(String value) {
        if (value == null) {
            return defaultValue;
        }
        //convert to type
        if (getType() == String.class) {
            return (T) value;
        }
        if (getType() == Integer.class) {
            return (T) Integer.valueOf(value);
        }
        if (getType() == Long.class) {
            return (T) Long.valueOf(value);
        }
        if (getType() == Double.class) {
            return (T) Double.valueOf(value);
        }
        if (getType() == Float.class) {
            return (T) Float.valueOf(value);
        }
        if (getType() == Short.class) {
            return (T) Short.valueOf(value);
        }
        if (getType() == Byte.class) {
            return (T) Byte.valueOf(value);
        }
        if (getType() == Character.class) {
            return (T) Character.valueOf(value.charAt(0));
        }
        if (getType() == Boolean.class) {
            return (T) Boolean.valueOf(value);
        }
        throw new UnsupportedOperationException("Not supported type: " + getType());
    }
}
