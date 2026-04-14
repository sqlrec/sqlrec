package com.sqlrec.udf;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.table.SetVariablesFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SetVariablesFunctionTest {
    private ExecuteContext context;

    @BeforeEach
    public void setUp() {
        context = new ExecuteContextImpl();
    }

    @Test
    public void testSetVariablesBasic() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"var1", "value1"});
        data.add(new Object[]{"var2", "value2"});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();
        CacheTable output = function.eval(context, input);

        assertNotNull(output);
        assertEquals("value1", context.getVariable("var1"));
        assertEquals("value2", context.getVariable("var2"));
    }

    @Test
    public void testSetVariablesEmptyTable() {
        List<Object[]> data = new ArrayList<>();

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();
        CacheTable output = function.eval(context, input);

        assertNotNull(output);
        assertEquals(0, context.getVariables().size());
    }

    @Test
    public void testSetVariablesWithNullValue() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"var1", "value1"});
        data.add(new Object[]{"var2", null});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        context.setVariable("var2", "old_value");

        SetVariablesFunction function = new SetVariablesFunction();
        CacheTable output = function.eval(context, input);

        assertNotNull(output);
        assertEquals("value1", context.getVariable("var1"));
        assertNull(context.getVariable("var2"));
    }

    @Test
    public void testSetVariablesOverwrite() {
        context.setVariable("var1", "old_value");

        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"var1", "new_value"});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();
        CacheTable output = function.eval(context, input);

        assertNotNull(output);
        assertEquals("new_value", context.getVariable("var1"));
    }

    @Test
    public void testSetVariablesNullContext() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"var1", "value1"});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.eval(null, input);
        });
    }

    @Test
    public void testSetVariablesNullInput() {
        SetVariablesFunction function = new SetVariablesFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.eval(context, null);
        });
    }

    @Test
    public void testSetVariablesWrongColumnCount() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"var1", "value1", "extra"});

        List<RelDataTypeField> dataFields = new ArrayList<>();
        dataFields.add(DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.VARCHAR));
        dataFields.add(DataTypeUtils.getRelDataTypeField("col2", 1, SqlTypeName.VARCHAR));
        dataFields.add(DataTypeUtils.getRelDataTypeField("col3", 2, SqlTypeName.VARCHAR));

        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.eval(context, input);
        });
    }

    @Test
    public void testSetVariablesWrongColumnType() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "value1"});

        List<RelDataTypeField> dataFields = new ArrayList<>();
        dataFields.add(DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.INTEGER));
        dataFields.add(DataTypeUtils.getRelDataTypeField("col2", 1, SqlTypeName.VARCHAR));

        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        SetVariablesFunction function = new SetVariablesFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.eval(context, input);
        });
    }

    private List<RelDataTypeField> createTestDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("key", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("value", 1, SqlTypeName.VARCHAR));
        return fields;
    }
}
