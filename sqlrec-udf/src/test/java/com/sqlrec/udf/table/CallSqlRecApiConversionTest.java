package com.sqlrec.udf.table;

import com.sqlrec.common.rest.ExecuteData;
import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CallSqlRecApiConversionTest {
    @Test
    void inferredApiValuesUseTheSameNullOnFailedConversionPolicy() {
        ReadonlyContext context = mock(ReadonlyContext.class);
        when(context.getVariables()).thenReturn(Map.of());
        ExecuteData response = new ExecuteData();
        response.setData(List.of(Map.of("score", 1L, "xs", List.of(1L), "flag", true,
                        "text", Map.of("html", "<tag>")),
                Map.of("score", 0.75d, "xs", List.of(0.5d, "bad"), "flag", "false"),
                Map.of("score", "bad", "xs", List.of(), "flag", "bad"),
                Map.of("score", 1.0d, "xs", new double[]{0.25d, 0.5d}, "flag", false)));
        CallSqlRecApiFunction function = new CallSqlRecApiFunction((url, data, params) -> response);
        CacheTable input = new CacheTable("input", Linq4j.singletonEnumerable(new Object[]{1L}),
                List.of(DataTypeUtils.getRelDataTypeField("id", 0, "BIGINT")));

        CacheTable output = function.evaluate(context, "http://unused.invalid", input);
        var rows = output.scan(null).toList();
        int score = DataTypeUtils.findFieldIndex(output.getDataFields(), "score");
        int xs = DataTypeUtils.findFieldIndex(output.getDataFields(), "xs");
        int flag = DataTypeUtils.findFieldIndex(output.getDataFields(), "flag");
        int text = DataTypeUtils.findFieldIndex(output.getDataFields(), "text");
        assertEquals("{\"html\":\"<tag>\"}", rows.get(0)[text]);
        assertEquals(1.0d, rows.get(0)[score]);
        assertEquals(0.75d, rows.get(1)[score]);
        assertNull(rows.get(2)[score]);
        assertEquals(Arrays.asList(0.5d, null), rows.get(1)[xs]);
        assertEquals(List.of(), rows.get(2)[xs]);
        assertEquals(List.of(0.25d, 0.5d), rows.get(3)[xs]);
        assertEquals(false, rows.get(1)[flag]);
        assertEquals(false, rows.get(2)[flag]);
    }
}
