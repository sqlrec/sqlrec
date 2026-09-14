package com.sqlrec.common.config;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigOptionTest {

    @Test
    public void testGetValueFromOptions_String() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.key", "actual_value");

        assertEquals("actual_value", option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_DefaultValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("default_value", option.getValue(Collections.emptyMap()));
    }

    @Test
    public void testGetValueFromOptionsDoesNotReadEnvironment() {
        ConfigOption<String> option = new ConfigOption<>(
                "PATH",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("default_value", option.getValue(Collections.emptyMap()));
    }

    @Test
    public void testRequiredValueFromOptionsDoesNotReadEnvironment() {
        ConfigOption<String> option = new ConfigOption<>(
                "PATH",
                null,
                "Test description",
                null,
                String.class
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> option.getValue(Collections.emptyMap()));
        assertEquals("PATH is not set", exception.getMessage());
    }

    @Test
    public void testGetValueWithEnvFallbackReadsEnvironmentBeforeDefault() {
        ConfigOption<String> option = new ConfigOption<>(
                "PATH",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals(System.getenv("PATH"),
                option.getValueWithEnvFallback(Collections.emptyMap()));
    }

    @Test
    public void testGetValueWithEnvFallbackPrefersOptions() {
        ConfigOption<String> option = new ConfigOption<>(
                "PATH",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("option_value",
                option.getValueWithEnvFallback(Map.of("PATH", "option_value")));
    }

    @Test
    public void testGetValueWithEnvFallbackUsesDefault() {
        ConfigOption<String> option = new ConfigOption<>(
                "NON_EXISTENT_ENV_VAR_WITH_FALLBACK_12345",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("default_value",
                option.getValueWithEnvFallback(Collections.emptyMap()));
    }

    @Test
    public void testGetValueFromOptions_NullOptions() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("default_value", option.getValue(null));
    }

    @Test
    public void testGetValueFromOptions_Integer() {
        ConfigOption<Integer> option = new ConfigOption<>(
                "test.int",
                100,
                "Test integer",
                null,
                Integer.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.int", "200");

        assertEquals(200, option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_Long() {
        ConfigOption<Long> option = new ConfigOption<>(
                "test.long",
                1000L,
                "Test long",
                null,
                Long.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.long", "2000");

        assertEquals(2000L, option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_Double() {
        ConfigOption<Double> option = new ConfigOption<>(
                "test.double",
                1.5,
                "Test double",
                null,
                Double.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.double", "2.5");

        assertEquals(2.5, option.getValue(options), 0.001);
    }

    @Test
    public void testGetValueFromOptions_Float() {
        ConfigOption<Float> option = new ConfigOption<>(
                "test.float",
                1.5f,
                "Test float",
                null,
                Float.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.float", "2.5");

        assertEquals(2.5f, option.getValue(options), 0.001);
    }

    @Test
    public void testGetValueFromOptions_Short() {
        ConfigOption<Short> option = new ConfigOption<>(
                "test.short",
                (short) 100,
                "Test short",
                null,
                Short.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.short", "200");

        assertEquals((short) 200, option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_Byte() {
        ConfigOption<Byte> option = new ConfigOption<>(
                "test.byte",
                (byte) 10,
                "Test byte",
                null,
                Byte.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.byte", "20");

        assertEquals((byte) 20, option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_Boolean() {
        ConfigOption<Boolean> option = new ConfigOption<>(
                "test.boolean",
                false,
                "Test boolean",
                null,
                Boolean.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.boolean", "true");

        assertTrue(option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_BooleanFalse() {
        ConfigOption<Boolean> option = new ConfigOption<>(
                "test.boolean",
                true,
                "Test boolean",
                null,
                Boolean.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.boolean", "false");

        assertFalse(option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_ValidValues() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.valid",
                "option1",
                "Test valid values",
                Arrays.asList("option1", "option2", "option3"),
                String.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.valid", "option2");

        assertEquals("option2", option.getValue(options));
    }

    @Test
    public void testGetValueFromOptions_InvalidValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.valid",
                "option1",
                "Test valid values",
                Arrays.asList("option1", "option2", "option3"),
                String.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.valid", "invalid_option");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> option.getValue(options)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }

    @Test
    public void testGetValueFromOptions_NoValueNoDefault() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.no.default",
                null,
                "Test no default",
                null,
                String.class
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> option.getValue(Collections.emptyMap())
        );
        assertTrue(exception.getMessage().contains("is not set"));
    }

    @Test
    public void testGetValueOrNullFromOptions() {
        ConfigOption<Integer> option = new ConfigOption<>(
                "test.optional", null, "Test optional value", null, Integer.class);

        assertEquals(10, option.getValueOrNull(Map.of("test.optional", "10")));
    }

    @Test
    public void testGetValueOrNullUsesDefaultValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.optional", "default_value", "Test optional value", null, String.class);

        assertEquals("default_value", option.getValueOrNull(Collections.emptyMap()));
    }

    @Test
    public void testGetValueOrNullWithoutDefaultReturnsNull() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.optional", null, "Test optional value", null, String.class);

        assertNull(option.getValueOrNull(null));
    }

    @Test
    public void testGetValueOrNullRejectsInvalidValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.optional", null, "Test optional value",
                Arrays.asList("one", "two"), String.class);

        assertThrows(IllegalArgumentException.class,
                () -> option.getValueOrNull(Map.of("test.optional", "three")));
    }

    @Test
    public void testIsSet() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key", null, "Test value presence", null, String.class);
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("test.key", null);

        assertAll(
                () -> assertFalse(option.isSet(null)),
                () -> assertFalse(option.isSet(Collections.emptyMap())),
                () -> assertFalse(option.isSet(nullValue)),
                () -> assertTrue(option.isSet(Map.of("test.key", ""))),
                () -> assertTrue(option.isSet(Map.of("test.key", "value")))
        );
    }

    @Test
    public void testSetDefaultValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key", "old", "Test mutable default", null, String.class);

        option.setDefaultValue("new");

        assertEquals("new", option.getDefaultValue());
        assertEquals("new", option.getValue(Collections.emptyMap()));
    }

    @Test
    public void testInvalidIntegerValue() {
        ConfigOption<Integer> option = new ConfigOption<>(
                "test.int", 0, "Test integer", null, Integer.class);

        assertThrows(NumberFormatException.class,
                () -> option.getValue(Map.of("test.int", "not-a-number")));
    }

    @Test
    public void testInvalidBooleanValueIsFalse() {
        ConfigOption<Boolean> option = new ConfigOption<>(
                "test.boolean", true, "Test boolean", null, Boolean.class);

        assertFalse(option.getValue(Map.of("test.boolean", "not-a-boolean")));
    }

    @Test
    public void testUnsupportedType() {
        ConfigOption<Object> option = new ConfigOption<>(
                "test.object", null, "Test unsupported type", null, Object.class);

        assertThrows(UnsupportedOperationException.class,
                () -> option.getValue(Map.of("test.object", "value")));
    }

    @Test
    public void testGetDefaultValue() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("default_value", option.getDefaultValue());
    }

    @Test
    public void testGetDescription() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("Test description", option.getDescription());
    }

    @Test
    public void testGetKey() {
        ConfigOption<String> option = new ConfigOption<>(
                "test.key",
                "default_value",
                "Test description",
                null,
                String.class
        );

        assertEquals("test.key", option.getKey());
    }

    @Test
    public void testGetType() {
        ConfigOption<Integer> option = new ConfigOption<>(
                "test.int",
                100,
                "Test integer",
                null,
                Integer.class
        );

        assertEquals(Integer.class, option.getType());
    }

    @Test
    public void testGetValue_EnvironmentVariable() {
        ConfigOption<String> option = new ConfigOption<>(
                "PATH",
                "default_path",
                "System path",
                null,
                String.class
        );

        String value = option.getValue();
        assertNotNull(value);
        assertNotEquals("default_path", value);
    }

    @Test
    public void testGetValue_EnvironmentVariableWithDefault() {
        ConfigOption<String> option = new ConfigOption<>(
                "NON_EXISTENT_ENV_VAR_12345",
                "default_value",
                "Non-existent env var",
                null,
                String.class
        );

        String value = option.getValue();
        assertEquals("default_value", value);
    }

    @Test
    public void testGetValue_EnvironmentVariableNoDefault() {
        ConfigOption<String> option = new ConfigOption<>(
                "NON_EXISTENT_ENV_VAR_67890",
                null,
                "Non-existent env var",
                null,
                String.class
        );

        assertThrows(IllegalArgumentException.class, option::getValue);
    }

    @Test
    public void testIntegerTypeConversion() {
        ConfigOption<Integer> option = new ConfigOption<>(
                "test.int",
                0,
                "Test integer conversion",
                null,
                Integer.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.int", "-100");

        assertEquals(-100, option.getValue(options));
    }

    @Test
    public void testLongTypeConversion() {
        ConfigOption<Long> option = new ConfigOption<>(
                "test.long",
                0L,
                "Test long conversion",
                null,
                Long.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.long", "9223372036854775807");

        assertEquals(Long.MAX_VALUE, option.getValue(options));
    }

    @Test
    public void testDoubleTypeConversion_Negative() {
        ConfigOption<Double> option = new ConfigOption<>(
                "test.double",
                0.0,
                "Test double conversion",
                null,
                Double.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.double", "-3.14159");

        assertEquals(-3.14159, option.getValue(options), 0.00001);
    }

    @Test
    public void testBooleanTypeConversion_TrueCase() {
        ConfigOption<Boolean> option = new ConfigOption<>(
                "test.bool",
                false,
                "Test boolean conversion",
                null,
                Boolean.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.bool", "true");

        assertTrue(option.getValue(options));
    }

    @Test
    public void testBooleanTypeConversion_FalseCase() {
        ConfigOption<Boolean> option = new ConfigOption<>(
                "test.bool",
                true,
                "Test boolean conversion",
                null,
                Boolean.class
        );

        Map<String, String> options = new HashMap<>();
        options.put("test.bool", "false");

        assertFalse(option.getValue(options));
    }
}
