package com.sqlrec.common.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonRowsTest {
    private final List<FieldSchema> fields = Arrays.asList(
            new FieldSchema("id", "INTEGER"), new FieldSchema("flag", "BOOLEAN"),
            new FieldSchema("missing", "STRING"));

    @Test
    void rawDecodingPreservesNamedProjectionAndLargeIntegralValues() {
        Object[] row = JsonRows.decode("{\"extra\":0,\"flag\":true,\"id\":9007199254740993}",
                fields, JsonRows.Decoding.RAW);
        assertArrayEquals(new Object[]{9007199254740993L, true, null}, row);
        List<FieldSchema> complexFields = Collections.singletonList(new FieldSchema("value", "STRING"));
        Object[] complex = JsonRows.decode("{\"value\":[1,{\"x\":2}]}", complexFields, JsonRows.Decoding.RAW);
        List<?> values = (List<?>) complex[0];
        assertEquals(1L, values.get(0));
        assertEquals(2L, ((Map<?, ?>) values.get(1)).get("x"));
        assertThrows(JsonSyntaxException.class,
                () -> JsonRows.decode("{\"id\":1,\"id\":2}", fields, JsonRows.Decoding.RAW));
    }

    @Test
    void coercionParsesScalarTextButPreservesBadCellsAndComplexJson() {
        List<JsonObject> objects = JsonRows.readObjects(
                "[null,1,[],{\"id\":\"42\",\"flag\":\"true\"},{\"id\":\"bad\",\"flag\":{\"x\":1}}]");
        List<Object[]> rows = JsonRows.decodeRows(objects, fields, JsonRows.Decoding.COERCE_SCALARS);
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{42, true, null}, rows.get(0));
        assertArrayEquals(new Object[]{null, "{\"x\":1}", null}, rows.get(1));
    }

    @Test
    void schemaMatchingKeepsIncompatiblePrimitivesNullAndArrayTextIntact() {
        List<FieldSchema> schema = Arrays.asList(new FieldSchema("number", "DOUBLE"),
                new FieldSchema("flag", "BOOLEAN"), new FieldSchema("array", "ARRAY<VARCHAR>"));
        assertArrayEquals(new Object[]{null, null, Arrays.asList("1", "true", "{}", "[2]", null)},
                JsonRows.decode("{\"number\":\"1\",\"flag\":1,\"array\":[1,true,{},[2],null]}",
                        schema, JsonRows.Decoding.MATCH_SCHEMA));
        assertArrayEquals(new Object[]{1.0d, true, null}, JsonRows.decode(
                "{\"number\":1,\"flag\":true,\"array\":\"scalar\"}", schema, JsonRows.Decoding.MATCH_SCHEMA));
    }

    @Test
    void objectCollectionDistinguishesEmptyArraysFromInvalidInputs() {
        assertEquals(1, JsonRows.readObjects("{}").size());
        assertTrue(JsonRows.readObjects("[null,1,[]]").isEmpty());
        assertThrows(IllegalArgumentException.class, () -> JsonRows.readObjects("null"));
        assertThrows(IllegalArgumentException.class, () -> JsonRows.readObjects("1"));
        assertThrows(JsonSyntaxException.class, () -> JsonRows.readObjects("{bad"));
    }
}
