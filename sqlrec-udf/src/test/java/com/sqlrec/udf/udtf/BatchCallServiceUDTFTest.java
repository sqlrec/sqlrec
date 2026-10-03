package com.sqlrec.udf.udtf;

import com.google.gson.JsonParser;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BatchCallServiceUDTFTest {

    @Test
    void finishFlushesTrailingPartialBatch() throws Exception {
        List<Integer> requestSizes = new ArrayList<>();
        BatchCallServiceUDTF function = new BatchCallServiceUDTF((url, body) -> {
            requestSizes.add(JsonParser.parseString(body).getAsJsonArray().size());
            return Collections.emptyMap();
        });
        List<Row> output = new ArrayList<>();
        function.setCollector(new ListCollector(output));
        function.open(null);

        function.eval("http://service", 2, "id", 1);
        function.eval("http://service", 2, "id", 2);
        function.eval("http://service", 2, "id", 3);
        function.finish();

        assertEquals(List.of(2, 1), requestSizes);
        assertEquals(3, output.size());
    }

    @Test
    void invalidFieldPairsKeepErrorPositionsAndInitializedSettings() throws Exception {
        BatchCallServiceUDTF function = new BatchCallServiceUDTF((url, body) -> Collections.emptyMap());
        List<Row> output = new ArrayList<>();
        function.setCollector(new ListCollector(output));
        function.open(null);

        assertEquals("fieldNameValuePairs must be in pairs of (fieldName, value)",
                assertThrows(IllegalArgumentException.class,
                        () -> function.eval("http://service", 2, "id")).getMessage());
        assertEquals("serviceUrl must remain constant for one function instance",
                assertThrows(IllegalArgumentException.class,
                        () -> function.eval("http://other", 2, "id", 1)).getMessage());
        assertEquals("batchSize must remain constant for one function instance",
                assertThrows(IllegalArgumentException.class,
                        () -> function.eval("http://service", 3, "id", 1)).getMessage());
        assertEquals("Field name at position 0 must be a String, but got: null",
                assertThrows(IllegalArgumentException.class,
                        () -> function.eval("http://service", 2, null, 1)).getMessage());
        assertEquals("Field name at position 2 must be a String, but got: java.lang.Integer",
                assertThrows(IllegalArgumentException.class,
                        () -> function.eval("http://service", 2, "id", 1, 42, "value")).getMessage());

        function.eval("http://service", 2, "id", 7);
        function.finish();
        assertEquals(1, output.size());
        assertEquals(Map.of("id", 7L), output.get(0).getField(0));
    }

    @Test
    void predictionsOverrideInputsAndBroadcastWithoutDroppingShortListRows() throws Exception {
        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("id", List.of(99L));
        predictions.put("label", "recommended");
        BatchCallServiceUDTF function = new BatchCallServiceUDTF((url, body) -> predictions);
        List<Row> output = new ArrayList<>();
        function.setCollector(new ListCollector(output));
        function.open(null);

        function.eval("http://service", 2, "id", 1, "count", (short) 3);
        function.eval("http://service", 2, "id", 2, "count", (byte) 4);

        assertEquals(2, output.size());
        assertEquals(Map.of("id", 99L, "count", 3L), output.get(0).getField(0));
        assertEquals(List.of("id", "count"),
                new ArrayList<>(((Map<?, ?>) output.get(0).getField(0)).keySet()));
        assertEquals(Map.of("id", 2L, "count", 4L), output.get(1).getField(0));
        for (Row row : output) {
            assertEquals(Map.of("label", "recommended"), row.getField(2));
        }
    }

    @Test
    void rowConversionPreservesArrayTypesAndMixedListFallbacks() throws Exception {
        BatchCallServiceUDTF function = new BatchCallServiceUDTF((url, body) -> null);
        List<Row> output = new ArrayList<>();
        function.setCollector(new ListCollector(output));
        function.open(null);

        function.eval("http://service", 1,
                "score", 0.5f,
                "ints", Arrays.asList(1, "invalid", null),
                "doubles", Arrays.asList(1.5f, "invalid", null),
                "intArray", new Integer[]{2, 3},
                "floatArray", new Float[]{2.5f},
                "names", new String[]{"a", "b"},
                "empty", Collections.emptyList(),
                "missing", null);

        Row row = output.get(0);
        assertEquals(Map.of("score", 0.5d), row.getField(1));
        Map<?, ?> longs = (Map<?, ?>) row.getField(3);
        Map<?, ?> doubles = (Map<?, ?>) row.getField(4);
        assertArrayEquals(new Long[]{1L, 0L, 0L}, (Long[]) longs.get("ints"));
        assertArrayEquals(new Long[]{2L, 3L}, (Long[]) longs.get("intArray"));
        assertArrayEquals(new Double[]{1.5d, 0.0d, 0.0d}, (Double[]) doubles.get("doubles"));
        assertArrayEquals(new Double[]{2.5d}, (Double[]) doubles.get("floatArray"));
        assertArrayEquals(new String[]{"a", "b"}, (String[]) ((Map<?, ?>) row.getField(5)).get("names"));
        for (int i = 0; i < row.getArity(); i++) {
            assertFalse(((Map<?, ?>) row.getField(i)).containsKey("empty"));
            assertFalse(((Map<?, ?>) row.getField(i)).containsKey("missing"));
        }
    }

    @Test
    void rowConversionRetainsTypedArraysAndUsesTheFirstListElement() throws Exception {
        BatchCallServiceUDTF function = new BatchCallServiceUDTF((url, body) -> null);
        List<Row> output = new ArrayList<>();
        function.setCollector(new ListCollector(output));
        function.open(null);
        Long[] longs = {1000L};
        Double[] doubles = {2.5d};
        String[] strings = {"original"};

        function.eval("http://service", 1,
                "longArray", longs, "doubleArray", doubles, "stringArray", strings,
                "longList", Arrays.asList(longs[0], 2.5d),
                "doubleList", Arrays.asList(doubles[0], 3L),
                "stringList", List.of("first", 2, true),
                "nullFirst", Arrays.asList(null, 1L),
                "unsupportedFirst", List.of(true, 1L));

        Row row = output.get(0);
        Map<?, ?> longArrays = (Map<?, ?>) row.getField(3);
        Map<?, ?> doubleArrays = (Map<?, ?>) row.getField(4);
        Map<?, ?> stringArrays = (Map<?, ?>) row.getField(5);
        assertSame(longs, longArrays.get("longArray"));
        assertSame(doubles, doubleArrays.get("doubleArray"));
        assertSame(strings, stringArrays.get("stringArray"));
        Long[] longList = (Long[]) longArrays.get("longList");
        Double[] doubleList = (Double[]) doubleArrays.get("doubleList");
        assertSame(longs[0], longList[0]);
        assertSame(doubles[0], doubleList[0]);
        assertArrayEquals(new Long[]{1000L, 0L}, longList);
        assertArrayEquals(new Double[]{2.5d, 0.0d}, doubleList);
        assertArrayEquals(new String[]{"first", "2", "true"}, (String[]) stringArrays.get("stringList"));
        for (int i = 0; i < row.getArity(); i++) {
            assertFalse(((Map<?, ?>) row.getField(i)).containsKey("nullFirst"));
            assertFalse(((Map<?, ?>) row.getField(i)).containsKey("unsupportedFirst"));
        }
    }

    private static final class ListCollector implements Collector<Row> {
        private final List<Row> rows;

        private ListCollector(List<Row> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(Row row) {
            rows.add(row);
        }

        @Override
        public void close() {
        }
    }
}
