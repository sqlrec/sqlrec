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

    public T getValue() {
        //get from environment variable
        String value = System.getenv(key);
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
