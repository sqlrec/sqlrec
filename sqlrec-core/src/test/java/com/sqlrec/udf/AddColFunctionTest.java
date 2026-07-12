package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.AddColFunction;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AddColFunctionTest {

    @Test
    public void testAddColBasic() {
        CacheTable input = createTable(new Object[][]{{1, "A"}, {2, "B"}});
        AddColFunction function = new AddColFunction();
        CacheTable output = function.evaluate(input, "status", "active");

        List<Object[]> result = collectRows(output);
        assertEquals(2, result.size());
        assertEquals(3, result.get(0).length);
        assertEquals(1, result.get(0)[0]);
        assertEquals("A", result.get(0)[1]);
        assertEquals("active", result.get(0)[2]);
        assertEquals(2, result.get(1)[0]);
        assertEquals("B", result.get(1)[1]);
        assertEquals("active", result.get(1)[2]);
    }

    @Test
    public void testAddColPreservesSchema() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        CacheTable output = function.evaluate(input, "new_col", "val");

        List<RelDataTypeField> fields = output.getDataFields();
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0).getName());
        assertEquals("name", fields.get(1).getName());
        assertEquals("new_col", fields.get(2).getName());
    }

    @Test
    public void testAddColEmptyTable() {
        CacheTable input = createTable(new Object[][]{});
        AddColFunction function = new AddColFunction();
        CacheTable output = function.evaluate(input, "extra", "default");

        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
        assertEquals(3, output.getDataFields().size());
    }

    @Test
    public void testAddColNullValue() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        CacheTable output = function.evaluate(input, "nullable_col", null);

        List<Object[]> result = collectRows(output);
        assertEquals(1, result.size());
        assertNull(result.get(0)[2]);
    }

    @Test
    public void testAddColEmptyNameThrows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "", "val");
        });
    }

    @Test
    public void testAddColNullNameThrows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, null, "val");
        });
    }

    @Test
    public void testAddColDuplicateNameThrows() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "name", "val");
        });
    }

    @Test
    public void testAddColDuplicateNameCaseInsensitive() {
        CacheTable input = createTable(new Object[][]{{1, "A"}});
        AddColFunction function = new AddColFunction();
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "NAME", "val");
        });
    }

    private CacheTable createTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createDataFields());
    }

    private List<RelDataTypeField> createDataFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("name", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
