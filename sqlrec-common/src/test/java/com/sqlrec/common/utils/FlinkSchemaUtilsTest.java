package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkSchemaUtilsTest {
    @Test
    void retainsCollectionElementTypesWithoutChangingScalarTypeNames() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("amount", DataTypes.DECIMAL(12, 2)),
                Column.physical("label", DataTypes.STRING()),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("statuses", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                Column.physical("child", DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()))));

        List<FieldSchema> fields = FlinkSchemaUtils.getFieldSchemas(schema);

        assertEquals("DECIMAL", fields.get(0).getType());
        assertEquals("VARCHAR", fields.get(1).getType());
        assertTrue(fields.get(2).getType().startsWith("ARRAY<"));
        assertTrue(fields.get(2).getType().contains("STRING"));
        assertTrue(fields.get(3).getType().startsWith("MAP<"));
        assertEquals(2, fields.get(3).getType().split("STRING", -1).length - 1);
        assertTrue(fields.get(4).getType().startsWith("ROW<"));
    }

    @Test
    void convertsDecodedRowsToFlinkInternalTypes() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("label", DataTypes.STRING()),
                Column.physical("amount", DataTypes.DECIMAL(8, 2)),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("children", DataTypes.MAP(DataTypes.STRING(),
                        DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())))));

        GenericRowData row = FlinkSchemaUtils.toRowData(new Object[]{
                "item", new BigDecimal("12.34"), Arrays.asList("first", null),
                Collections.singletonMap("one", Collections.singletonMap("name", "child"))
        }, schema.getColumnDataTypes());

        assertEquals(StringData.fromString("item"), row.getString(0));
        assertEquals(new BigDecimal("12.34"), row.getDecimal(1, 8, 2).toBigDecimal());
        assertEquals(StringData.fromString("first"), row.getArray(2).getString(0));
        assertNull(row.getArray(2).getString(1));
        MapData children = row.getMap(3);
        assertEquals(StringData.fromString("one"), children.keyArray().getString(0));
        RowData child = children.valueArray().getRow(0, 1);
        assertEquals(StringData.fromString("child"), child.getString(0));
        assertEquals(StringData.fromString("direct"),
                FlinkSchemaUtils.toFlinkValue("direct", DataTypes.STRING().getLogicalType()));
        assertThrows(IllegalArgumentException.class,
                () -> FlinkSchemaUtils.toRowData(new Object[]{"too short"}, schema.getColumnDataTypes()));
    }
}
