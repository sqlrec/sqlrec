package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
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
    void normalizesDecodedNumbersBeforeFlinkReadsThem() {
        List<DataType> types = Arrays.asList(DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(),
                DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE());
        for (Number source : Arrays.<Number>asList(7L, 7, 7.0d, 7.0f, new BigDecimal("7"))) {
            GenericRowData row = FlinkSchemaUtils.toRowData(
                    new Object[]{source, source, source, source, source, source}, types);
            assertEquals((byte) 7, row.getByte(0));
            assertEquals((short) 7, row.getShort(1));
            assertEquals(7, row.getInt(2));
            assertEquals(7L, row.getLong(3));
            assertEquals(7.0f, row.getFloat(4));
            assertEquals(7.0d, row.getDouble(5));
        }
        GenericRowData nulls = FlinkSchemaUtils.toRowData(new Object[types.size()], types);
        for (int i = 0; i < types.size(); i++) {
            assertTrue(nulls.isNullAt(i));
        }
    }

    @Test
    void normalizesNumbersInsideNestedCollections() {
        List<Long> ints = Arrays.asList(7L, null);
        List<List<Double>> floats = Arrays.asList(Arrays.asList(0.25d, null), null, Collections.emptyList());
        Map<Long, Double> scores = Collections.singletonMap(3L, 0.5d);
        GenericRowData row = FlinkSchemaUtils.toRowData(new Object[]{ints, floats, scores}, Arrays.asList(
                DataTypes.ARRAY(DataTypes.INT()),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.FLOAT())),
                DataTypes.MAP(DataTypes.INT(), DataTypes.FLOAT())));

        assertEquals(7, row.getArray(0).getInt(0));
        assertTrue(row.getArray(0).isNullAt(1));
        assertEquals(0.25f, row.getArray(1).getArray(0).getFloat(0));
        assertTrue(row.getArray(1).getArray(0).isNullAt(1));
        assertTrue(row.getArray(1).isNullAt(1));
        assertEquals(0, row.getArray(1).getArray(2).size());
        assertEquals(3, row.getMap(2).keyArray().getInt(0));
        assertEquals(0.5f, row.getMap(2).valueArray().getFloat(0));
        assertEquals(Long.class, ints.get(0).getClass());
        assertEquals(Double.class, floats.get(0).get(0).getClass());
    }

    @Test
    void retainsCollectionElementTypesAndDecimalBounds() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("amount", DataTypes.DECIMAL(12, 2)),
                Column.physical("label", DataTypes.STRING()),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("statuses", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

        List<FieldSchema> fields = FlinkSchemaUtils.getFieldSchemas(schema);

        assertEquals(12, DataTypeUtils.getRelDataType(fields.get(0).getType()).getPrecision());
        assertEquals(2, DataTypeUtils.getRelDataType(fields.get(0).getType()).getScale());
        assertEquals("VARCHAR", fields.get(1).getType());
        assertTrue(fields.get(2).getType().startsWith("ARRAY<"));
        assertTrue(fields.get(2).getType().contains("STRING"));
        assertTrue(fields.get(3).getType().startsWith("MAP<"));
        assertEquals(2, fields.get(3).getType().split("STRING", -1).length - 1);
    }

    @Test
    void rejectsRowTypesInConnectorSchemas() {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
        for (DataType type : List.of(rowType, DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(rowType)))) {
            ResolvedSchema schema = ResolvedSchema.of(Column.physical("value", type));
            var error = assertThrows(ValidationException.class, () -> FlinkSchemaUtils.getFieldSchemas(schema));
            assertEquals("ROW column types are not supported", error.getMessage());
        }
    }

    @Test
    void convertsDecodedRowsToFlinkInternalTypes() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("label", DataTypes.STRING()),
                Column.physical("amount", DataTypes.DECIMAL(8, 2)),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("labels", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

        GenericRowData row = FlinkSchemaUtils.toRowData(new Object[]{
                "item", new BigDecimal("12.34"), Arrays.asList("first", null),
                Collections.singletonMap("one", "child")
        }, schema.getColumnDataTypes());

        assertEquals(StringData.fromString("item"), row.getString(0));
        assertEquals(new BigDecimal("12.34"), row.getDecimal(1, 8, 2).toBigDecimal());
        assertEquals(StringData.fromString("first"), row.getArray(2).getString(0));
        assertNull(row.getArray(2).getString(1));
        MapData labels = row.getMap(3);
        assertEquals(StringData.fromString("one"), labels.keyArray().getString(0));
        assertEquals(StringData.fromString("child"), labels.valueArray().getString(0));
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
    void recursivelyConvertsCollectionsInMapOrder() {
        Map<String, List<String>> labels = new LinkedHashMap<>();
        labels.put("second", Arrays.asList("child", null));
        labels.put("first", Collections.emptyList());
        LogicalType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())).getLogicalType();

        MapData converted = (MapData) FlinkSchemaUtils.toFlinkValue(labels, type);

        assertEquals(StringData.fromString("second"), converted.keyArray().getString(0));
        assertEquals(StringData.fromString("first"), converted.keyArray().getString(1));
        assertEquals(StringData.fromString("child"), converted.valueArray().getArray(0).getString(0));
        assertTrue(converted.valueArray().getArray(0).isNullAt(1));
        assertEquals(0, converted.valueArray().getArray(1).size());
        assertEquals(Arrays.asList("second", "first"), new ArrayList<>(labels.keySet()));
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
