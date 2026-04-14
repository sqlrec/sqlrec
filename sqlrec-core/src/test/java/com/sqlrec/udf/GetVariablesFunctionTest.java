package com.sqlrec.udf;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.table.GetVariablesFunction;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class GetVariablesFunctionTest {
    private ExecuteContext context;

    @BeforeEach
    public void setUp() {
        context = new ExecuteContextImpl();
    }

    @Test
    public void testGetVariablesEmpty() {
        GetVariablesFunction function = new GetVariablesFunction();
        CacheTable output = function.eval(context);

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(0, resultData.size());

        List<RelDataTypeField> dataFields = output.getDataFields();
        assertEquals(2, dataFields.size());
        assertEquals("key", dataFields.get(0).getName());
        assertEquals("value", dataFields.get(1).getName());
    }

    @Test
    public void testGetVariablesSingleVariable() {
        context.setVariable("var1", "value1");

        GetVariablesFunction function = new GetVariablesFunction();
        CacheTable output = function.eval(context);

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(1, resultData.size());
        assertArrayEquals(new Object[]{"var1", "value1"}, resultData.get(0));
    }

    @Test
    public void testGetVariablesMultipleVariables() {
        context.setVariable("var1", "value1");
        context.setVariable("var2", "value2");
        context.setVariable("var3", "value3");

        GetVariablesFunction function = new GetVariablesFunction();
        CacheTable output = function.eval(context);

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(3, resultData.size());

        Map<String, String> resultMap = new HashMap<>();
        for (Object[] row : resultData) {
            resultMap.put((String) row[0], (String) row[1]);
        }

        assertEquals("value1", resultMap.get("var1"));
        assertEquals("value2", resultMap.get("var2"));
        assertEquals("value3", resultMap.get("var3"));
    }

    @Test
    public void testGetVariablesWithNullValue() {
        context.setVariable("var1", "value1");
        context.setVariable("var2", null);

        GetVariablesFunction function = new GetVariablesFunction();
        CacheTable output = function.eval(context);

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(1, resultData.size());
        assertEquals("var1", resultData.get(0)[0]);
        assertEquals("value1", resultData.get(0)[1]);
    }

    @Test
    public void testGetVariablesNullContext() {
        GetVariablesFunction function = new GetVariablesFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.eval(null);
        });
    }
}
