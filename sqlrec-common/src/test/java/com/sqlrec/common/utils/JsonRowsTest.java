package com.sqlrec.common.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonRowsTest {
    @Test
    void numericLiteralsConvertWithoutLosingTheirValueOrOriginalText() {
        List<FieldSchema> schema = List.of(new FieldSchema("id", "INTEGER"),
                new FieldSchema("count", "BIGINT"), new FieldSchema("large", "BIGINT"),
                new FieldSchema("amount", "DECIMAL(18, 6)"), new FieldSchema("text", "STRING"),
                new FieldSchema("ids", "ARRAY<INTEGER>"),
                new FieldSchema("child", "ROW<n BIGINT>"));
        assertArrayEquals(new Object[]{1, 100L, 9007199254740993L, new BigDecimal("9876543210.123400"),
                        "1e2", Arrays.asList(1, 100, -2, null, null), Map.of("n", 200L)},
                JsonRows.decode("{\"id\":1.0,\"count\":1e2,\"large\":9007199254740993,"
                        + "\"amount\":9876543210.123400,\"text\":1e2,\"ids\":[1.0,1e2,-2.0,\"bad\",null],"
                        + "\"child\":{\"n\":2e2}}", schema));
    }

    @Test
    void usesExistingScalarConversionsAndKeepsThrownConversionsNull() {
        List<FieldSchema> schema = List.of(new FieldSchema("whole", "INTEGER"),
                new FieldSchema("fraction", "BIGINT"), new FieldSchema("overflow", "INTEGER"),
                new FieldSchema("infinite", "DOUBLE"), new FieldSchema("boolean", "BOOLEAN"),
                new FieldSchema("zero", "BOOLEAN"), new FieldSchema("one", "BOOLEAN"),
                new FieldSchema("decimal", "DECIMAL(4, 2)"), new FieldSchema("object", "DOUBLE"));
        assertArrayEquals(new Object[]{null, 0L, Integer.MIN_VALUE, Double.POSITIVE_INFINITY, false, false, false,
                new BigDecimal("12.345"), null}, JsonRows.decode(
                "{\"whole\":\"1e2\",\"fraction\":0.75,\"overflow\":2147483648,\"infinite\":1e400,"
                        + "\"boolean\":\"bad\",\"zero\":0,\"one\":1.0,\"decimal\":12.345,\"object\":{}}", schema));
        assertArrayEquals(new Object[]{null, null, null, null, null, null, null, null, null},
                JsonRows.decode("{}", schema));
    }

    @Test
    void recursivelyConvertsMapsAndNamedRowsAndKeepsInvalidChildrenNull() {
        List<FieldSchema> schema = List.of(new FieldSchema("values",
                "MAP<INTEGER, ARRAY<ROW<`User Id` BIGINT, Score FLOAT, Flag BOOLEAN>>>"));
        Object[] row = JsonRows.decode("{\"values\":{\"7\":[{\"User Id\":\"12\",\"Score\":\"0.25\","
                + "\"Flag\":\"bad\"},\"invalid row\",null]}}", schema);
        Map<String, Object> child = new java.util.LinkedHashMap<>();
        child.put("User Id", 12L);
        child.put("Score", 0.25d);
        child.put("Flag", false);
        assertEquals(Map.of(7, Arrays.asList(child, null, null)), row[0]);
        assertNull(JsonRows.decode("{\"values\":{\"bad\":[]}}", schema)[0]);
        assertNull(JsonRows.decode("{\"values\":{\"01\":[],\"1\":[]}}", schema)[0]);
    }

    @Test
    void coercionDecodesDeclaredArraysUsingTheirElementTypes() {
        List<FieldSchema> schema = List.of(new FieldSchema("ints", "ARRAY<INTEGER>"),
                new FieldSchema("longs", "ARRAY<BIGINT>"), new FieldSchema("scores", "ARRAY<FLOAT>"),
                new FieldSchema("amounts", "ARRAY<DECIMAL(12, 3)>"),
                new FieldSchema("flags", "ARRAY<BOOLEAN>"), new FieldSchema("tags", "ARRAY<VARCHAR(16)>"));
        Object[] row = JsonRows.decode("{\"ints\":[1,\"2\",null,\"bad\"],\"longs\":[9007199254740993],"
                        + "\"scores\":[0.25,\"0.5\",null],\"amounts\":[\"42.125\",43.500],"
                        + "\"flags\":[true,\"false\",null],\"tags\":[\"red\",7,{\"x\":1}]}",
                schema);

        assertArrayEquals(new Object[]{Arrays.asList(1, 2, null, null), List.of(9007199254740993L),
                Arrays.asList(0.25d, 0.5d, null), List.of(new BigDecimal("42.125"), new BigDecimal("43.500")),
                Arrays.asList(true, false, null), List.of("red", "7", "{\"x\":1}")}, row);
    }

    @Test
    void coercionPreservesNestedArraysEmptyArraysAndNulls() {
        List<FieldSchema> schema = List.of(new FieldSchema("nested", " array<array<float not null>> not null "),
                new FieldSchema("empty", "ARRAY<INTEGER>"), new FieldSchema("nulls", "ARRAY<INTEGER>"),
                new FieldSchema("missing", "ARRAY<INTEGER>"));
        Object[] row = JsonRows.decode("{\"nested\":[[0.25,null],null,[],\"bad\"],\"empty\":[],\"nulls\":null}",
                schema);

        assertArrayEquals(new Object[]{Arrays.asList(Arrays.asList(0.25d, null), null, List.of(), null),
                List.of(), null, null}, row);
    }

    @Test
    void coercionKeepsInvalidArrayValuesNullAndTextColumnsAsJson() {
        List<FieldSchema> schema = List.of(new FieldSchema("xs", "ARRAY<INTEGER>"),
                new FieldSchema("object", "ARRAY<INTEGER>"), new FieldSchema("text", "STRING"));
        assertArrayEquals(new Object[]{List.of(1, 2), null, "[1,2]"}, JsonRows.decode(
                "{\"xs\":\"[1,2]\",\"object\":{},\"text\":[1,2]}", schema));
        assertArrayEquals(new Object[]{Arrays.asList(1, null, null, null), List.of(), "{\"a\":1}"},
                JsonRows.decode("{\"xs\":[1,\"bad\",{},[]],\"object\":[],\"text\":{\"a\":1}}",
                        schema));
    }

    private final List<FieldSchema> fields = Arrays.asList(
            new FieldSchema("id", "INTEGER"), new FieldSchema("flag", "BOOLEAN"),
            new FieldSchema("missing", "STRING"));

    @Test
    void decodingUsesDeclaredTypesAndPreservesLargeIntegralValues() {
        Object[] row = JsonRows.decode("{\"extra\":0,\"flag\":true,\"id\":9007199254740993}",
                List.of(new FieldSchema("id", "BIGINT"), new FieldSchema("flag", "BOOLEAN"),
                        new FieldSchema("missing", "STRING")));
        assertArrayEquals(new Object[]{9007199254740993L, true, null}, row);
        List<FieldSchema> complexFields = Collections.singletonList(new FieldSchema("value", "STRING"));
        Object[] complex = JsonRows.decode("{\"value\":[1,{\"x\":2}]}", complexFields);
        assertEquals("[1,{\"x\":2}]", complex[0]);
        assertEquals("{\"html\":\"<tag>\",\"empty\":null}", JsonRows.decode(
                "{\"value\":{\"html\":\"<tag>\",\"empty\":null}}", complexFields)[0]);
        assertThrows(JsonSyntaxException.class,
                () -> JsonRows.decode("{\"id\":1,\"id\":2}", fields));
        assertThrows(JsonSyntaxException.class,
                () -> JsonRows.decode("{\"id\":null,\"id\":2}", fields));
    }

    @Test
    void coercionParsesScalarTextButPreservesBadCellsAndComplexJson() {
        List<JsonObject> objects = JsonRows.readObjects(
                "[null,1,[],{\"id\":\"42\",\"flag\":\"true\"},{\"id\":\"bad\",\"flag\":{\"x\":1}}]");
        List<Object[]> rows = JsonRows.decodeRows(objects, fields);
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{42, true, null}, rows.get(0));
        assertArrayEquals(new Object[]{null, null, null}, rows.get(1));
    }

    @Test
    void coercionResolvesNullabilityAndParameterizedTypes() {
        List<FieldSchema> fields = List.of(new FieldSchema("id", "BIGINT NOT NULL"),
                new FieldSchema("name", "VARCHAR(16)"),
                new FieldSchema("amount", "DECIMAL(12, 3) NOT NULL"));
        assertArrayEquals(new Object[]{1L, "alice", new BigDecimal("42.125")},
                JsonRows.decode("{\"id\":1,\"name\":\"alice\",\"amount\":\"42.125\"}",
                        fields));
        assertArrayEquals(new Object[]{2L, "bob", new BigDecimal("43.500")},
                JsonRows.decode("{\"id\":\"2\",\"name\":\"bob\",\"amount\":43.500}",
                        fields));
        assertArrayEquals(new Object[]{null, null, null},
                JsonRows.decode("{\"id\":\"bad\",\"amount\":\"bad\"}",
                        fields));
    }

    @Test
    void decodingAttemptsPrimitiveConversionsAndPreservesArrayText() {
        List<FieldSchema> schema = Arrays.asList(new FieldSchema("number", "DOUBLE"),
                new FieldSchema("flag", "BOOLEAN"), new FieldSchema("array", "ARRAY<VARCHAR>"));
        assertArrayEquals(new Object[]{1.0d, false, Arrays.asList("1", "true", "{}", "[2]", null)},
                JsonRows.decode("{\"number\":\"1\",\"flag\":1,\"array\":[1,true,{},[2],null]}",
                        schema));
        assertArrayEquals(new Object[]{1.0d, true, null}, JsonRows.decode(
                "{\"number\":1,\"flag\":true,\"array\":\"scalar\"}", schema));
    }

    @Test
    void objectCollectionDistinguishesEmptyArraysFromInvalidInputs() {
        assertEquals(1, JsonRows.readObjects("{}").size());
        assertTrue(JsonRows.readObjects("[null,1,[]]").isEmpty());
        assertThrows(IllegalArgumentException.class, () -> JsonRows.readObjects("null"));
        assertThrows(IllegalArgumentException.class, () -> JsonRows.readObjects("1"));
        assertThrows(JsonSyntaxException.class, () -> JsonRows.readObjects("{bad"));
        assertThrows(JsonSyntaxException.class, () -> JsonRows.decode("[[\"id\",1]]", fields));
        assertThrows(JsonSyntaxException.class, () -> JsonRows.decode("{} {}", fields));
    }
}
