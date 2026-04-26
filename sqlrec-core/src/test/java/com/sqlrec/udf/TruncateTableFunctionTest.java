package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.TruncateTableFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TruncateTableFunctionTest {

    @Test
    public void testTruncateBasic() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});
        data.add(new Object[]{2, "Bob", 21});
        data.add(new Object[]{3, "Charlie", 22});
        data.add(new Object[]{4, "David", 23});
        data.add(new Object[]{5, "Eve", 24});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "1", "3");

        assertNotNull(output);
        assertEquals(dataFields, output.getDataFields());

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(2, resultData.size());
        assertArrayEquals(new Object[]{2, "Bob", 21}, resultData.get(0));
        assertArrayEquals(new Object[]{3, "Charlie", 22}, resultData.get(1));
    }

    @Test
    public void testTruncateFromStart() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});
        data.add(new Object[]{2, "Bob", 21});
        data.add(new Object[]{3, "Charlie", 22});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "0", "2");

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(2, resultData.size());
        assertArrayEquals(new Object[]{1, "Alice", 20}, resultData.get(0));
        assertArrayEquals(new Object[]{2, "Bob", 21}, resultData.get(1));
    }

    @Test
    public void testTruncateToEnd() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});
        data.add(new Object[]{2, "Bob", 21});
        data.add(new Object[]{3, "Charlie", 22});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "1", "10");

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(2, resultData.size());
        assertArrayEquals(new Object[]{2, "Bob", 21}, resultData.get(0));
        assertArrayEquals(new Object[]{3, "Charlie", 22}, resultData.get(1));
    }

    @Test
    public void testTruncateEmptyResult() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});
        data.add(new Object[]{2, "Bob", 21});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "5", "10");

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(0, resultData.size());
    }

    @Test
    public void testTruncateEmptyTable() {
        List<Object[]> data = new ArrayList<>();

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "0", "5");

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(0, resultData.size());
    }

    @Test
    public void testTruncateStartEqualsEnd() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});
        data.add(new Object[]{2, "Bob", 21});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();
        CacheTable output = function.evaluate(input, "1", "1");

        assertNotNull(output);

        List<Object[]> resultData = new ArrayList<>();
        output.scan(null).forEach(resultData::add);

        assertEquals(0, resultData.size());
    }

    @Test
    public void testTruncateNullStart() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, null, "5");
        });
    }

    @Test
    public void testTruncateNullEnd() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "0", null);
        });
    }

    @Test
    public void testTruncateNegativeStart() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "-1", "5");
        });
    }

    @Test
    public void testTruncateNegativeEnd() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "0", "-1");
        });
    }

    @Test
    public void testTruncateStartGreaterThanEnd() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "5", "2");
        });
    }

    @Test
    public void testTruncateInvalidStartFormat() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "abc", "5");
        });
    }

    @Test
    public void testTruncateInvalidEndFormat() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{1, "Alice", 20});

        List<RelDataTypeField> dataFields = createTestDataFields();
        CacheTable input = new CacheTable("input", Linq4j.asEnumerable(data), dataFields);

        TruncateTableFunction function = new TruncateTableFunction();

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "0", "xyz");
        });
    }

    private List<RelDataTypeField> createTestDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("age", 2, SqlTypeName.INTEGER));
        return fields;
    }
}
