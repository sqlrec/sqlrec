package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that when an IF statement has only a then branch (cache statement) and the condition is false,
 * an already existing cache table with the same name must keep the exact same schema as the then clause.
 */
public class IfBindableSchemaTest {

    private abstract static class TestBindable extends BindableInterface {
        @Override
        public List<RelDataTypeField> getReturnDataFields() {
            return Collections.singletonList(
                    DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.VARCHAR)
            );
        }

        @Override
        public boolean isParallelizable() {
            return true;
        }

        @Override
        public Set<String> getReadTables() {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getWriteTables() {
            return Collections.emptySet();
        }
    }

    private static CalciteBindable falseCondition() {
        List<Object[]> rowList = new ArrayList<>();
        rowList.add(new Object[]{Boolean.FALSE});
        return new CalciteBindable(
                new HashMap<>(),
                dataContext -> Linq4j.asEnumerable(rowList),
                null, null, null, null, null
        );
    }

    private static TestBindable returningRows(Object... values) {
        List<Object[]> rows = new ArrayList<>();
        for (Object value : values) {
            rows.add(new Object[]{value});
        }
        return new TestBindable() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
                return Linq4j.asEnumerable(rows);
            }
        };
    }

    private static void registerExistingTable(CalciteSchema schema, String tableName,
                                              List<RelDataTypeField> fields, List<Object[]> rows) {
        schema.add(tableName, new CacheTable(tableName, Linq4j.asEnumerable(rows), fields));
    }

    private static List<Object[]> scanTable(CacheTable table) {
        List<Object[]> data = new ArrayList<>();
        table.scan(null).forEach(data::add);
        return data;
    }

    @Test
    public void testConditionFalseKeepsExistingTableWhenSchemaSame() {
        IfBindable ifBindable = new IfBindable(
                falseCondition(),
                new CacheTableBindable("t", returningRows("new")),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        registerExistingTable(schema, "t",
                Collections.singletonList(DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.VARCHAR)),
                Collections.singletonList(new Object[]{"old"}));

        Enumerable<Object[]> result = ifBindable.bind(schema, context);

        assertEquals(0, result.count());
        CacheTable table = SchemaUtils.tryGetCacheTable("t", schema);
        assertNotNull(table, "existing cache table should be kept");
        List<Object[]> data = scanTable(table);
        assertEquals(1, data.size());
        assertEquals("old", data.get(0)[0]);
    }

    @Test
    public void testConditionFalseThrowsWhenExistingTableHasMoreColumns() {
        // old table has [col1, extra]: prefix-compatible under the lenient check, must fail under the strict check
        IfBindable ifBindable = new IfBindable(
                falseCondition(),
                new CacheTableBindable("t", returningRows("new")),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        registerExistingTable(schema, "t",
                Arrays.asList(
                        DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.VARCHAR),
                        DataTypeUtils.getRelDataTypeField("extra", 1, SqlTypeName.VARCHAR)
                ),
                Collections.singletonList(new Object[]{"old", "x"}));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
        assertEquals("field count not equal: 1 != 2", ex.getMessage());
    }

    @Test
    public void testConditionFalseThrowsWhenExistingTableHasFewerColumns() {
        IfBindable ifBindable = new IfBindable(
                falseCondition(),
                new CacheTableBindable("t", returningRows("new")),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        registerExistingTable(schema, "t",
                Collections.emptyList(),
                Collections.emptyList());

        RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
        assertEquals("field count not equal: 1 != 0", ex.getMessage());
    }

    @Test
    public void testConditionFalseThrowsWhenFieldNameDiffers() {
        IfBindable ifBindable = new IfBindable(
                falseCondition(),
                new CacheTableBindable("t", returningRows("new")),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        registerExistingTable(schema, "t",
                Collections.singletonList(DataTypeUtils.getRelDataTypeField("other", 0, SqlTypeName.VARCHAR)),
                Collections.singletonList(new Object[]{"old"}));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
        assertEquals("field name not equal: col1 != other", ex.getMessage());
    }

    @Test
    public void testConditionFalseThrowsWhenFieldTypeDiffers() {
        IfBindable ifBindable = new IfBindable(
                falseCondition(),
                new CacheTableBindable("t", returningRows("new")),
                null,
                false
        );

        ExecuteContextImpl context = new ExecuteContextImpl();
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        registerExistingTable(schema, "t",
                Collections.singletonList(DataTypeUtils.getRelDataTypeField("col1", 0, SqlTypeName.BIGINT)),
                Collections.singletonList(new Object[]{1L}));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> ifBindable.bind(schema, context));
        assertTrue(ex.getMessage().contains("field type not equal"),
                "unexpected message: " + ex.getMessage());
    }
}
