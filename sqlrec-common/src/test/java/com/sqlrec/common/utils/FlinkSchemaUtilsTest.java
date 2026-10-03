package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    @Test
    void readsSupportedArrayElementsWithoutDroppingNulls() {
        DataType[] elementTypes = {
                DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE(),
                DataTypes.CHAR(4), DataTypes.STRING()
        };
        Object[] internalValues = {
                7, 7L, 7.5f, 7.5d, StringData.fromString("char"), StringData.fromString("text")
        };
        Object[] expectedValues = {7, 7L, 7.5f, 7.5d, "char", "text"};
        for (int i = 0; i < elementTypes.length; i++) {
            GenericRowData row = GenericRowData.of(
                    new GenericArrayData(new Object[]{internalValues[i], null}));
            assertEquals(Arrays.asList(expectedValues[i], null), FlinkSchemaUtils.typeConversion(
                    DataTypes.ARRAY(elementTypes[i]).getLogicalType(), row, 0));
        }
    }

    @Test
    void unsupportedArrayTypesFailOnlyWhenReadingANonNullElement() {
        LogicalType type = DataTypes.ARRAY(DataTypes.BOOLEAN()).getLogicalType();
        assertEquals(Collections.emptyList(), FlinkSchemaUtils.typeConversion(
                type, GenericRowData.of(new GenericArrayData(new Object[0])), 0));
        assertEquals(Arrays.asList(null, null), FlinkSchemaUtils.typeConversion(
                type, GenericRowData.of(new GenericArrayData(new Object[]{null, null})), 0));
        UnsupportedOperationException error = assertThrows(UnsupportedOperationException.class,
                () -> FlinkSchemaUtils.typeConversion(type,
                        GenericRowData.of(new GenericArrayData(new Object[]{null, true})), 0));
        assertEquals("Unsupported array element type: BOOLEAN", error.getMessage());
        assertNull(FlinkSchemaUtils.toFlinkValue(null, null));
        assertNull(FlinkSchemaUtils.typeConversion(null, new GenericRowData(1), 0));
    }

    @Test
    void recursivelyConvertsCollectionsInMapAndSchemaOrder() {
        DataType childType = DataTypes.ROW(
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("missing", DataTypes.INT()));
        Map<String, Object> child = new LinkedHashMap<>();
        child.put("extra", 99);
        child.put("name", "child");
        Map<String, Object> children = new LinkedHashMap<>();
        children.put("second", Arrays.asList(child, null));
        children.put("first", Collections.emptyList());
        LogicalType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(childType)).getLogicalType();

        MapData converted = (MapData) FlinkSchemaUtils.toFlinkValue(children, type);

        assertEquals(StringData.fromString("second"), converted.keyArray().getString(0));
        assertEquals(StringData.fromString("first"), converted.keyArray().getString(1));
        RowData row = converted.valueArray().getArray(0).getRow(0, 2);
        assertEquals(StringData.fromString("child"), row.getString(0));
        assertTrue(row.isNullAt(1));
        assertTrue(converted.valueArray().getArray(0).isNullAt(1));
        assertEquals(0, converted.valueArray().getArray(1).size());
        assertEquals(Arrays.asList("second", "first"), new ArrayList<>(children.keySet()));
        assertEquals(Arrays.asList("extra", "name"), new ArrayList<>(child.keySet()));
        assertEquals("child", child.get("name"));
    }
    @Test
    public void testFlinkArrayConversionPreservesNullElements() {
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, new GenericArrayData(new Integer[]{1, null, 3}));

        Object result = FlinkSchemaUtils.typeConversion(
                new ArrayType(new IntType()), rowData, 0);

        assertEquals(Arrays.asList(1, null, 3), result);
    }
}
