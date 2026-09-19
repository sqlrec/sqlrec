package com.sqlrec.udf.udtf;

import com.google.gson.JsonParser;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
