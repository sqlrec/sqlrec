package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FlinkSchemaUtils {
    public static List<FieldSchema> getFieldSchemas(ResolvedSchema schema) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            LogicalType type = col.getDataType().getLogicalType();
            String typeName;
            switch (type.getTypeRoot()) {
                case ARRAY:
                case MAP:
                case ROW:
                    typeName = type.asSummaryString();
                    break;
                default:
                    typeName = type.getTypeRoot().name();
            }
            fieldSchemas.add(new FieldSchema(col.getName(), typeName));
        }
        return fieldSchemas;
    }

    public static String getPrimaryKey(ResolvedSchema schema) {
        Optional<UniqueConstraint> uniqueConstraint = schema.getPrimaryKey();
        if (uniqueConstraint.isPresent()) {
            List<String> primaryKeys = uniqueConstraint.get().getColumns();
            if (primaryKeys.size() != 1) {
                throw new IllegalArgumentException("table must have only one primary key");
            }
            return primaryKeys.get(0);
        }
        throw new IllegalArgumentException("table must have primary key");
    }

    public static <T> ConfigOption<T> toFlinkConfigOption(com.sqlrec.common.config.ConfigOption<T> configOption) {
        ConfigOptions.OptionBuilder optionBuilder = ConfigOptions.key(configOption.getKey());
        if (configOption.getDefaultValue() != null) {
            return optionBuilder.defaultValue(configOption.getDefaultValue());
        }

        Class<T> type = configOption.getType();
        if (type == String.class || type == Character.class) {
            return (ConfigOption<T>) optionBuilder.stringType().noDefaultValue();
        } else if (type == Integer.class || type == Short.class || type == Byte.class) {
            return (ConfigOption<T>) optionBuilder.intType().noDefaultValue();
        } else if (type == Long.class) {
            return (ConfigOption<T>) optionBuilder.longType().noDefaultValue();
        } else if (type == Double.class) {
            return (ConfigOption<T>) optionBuilder.doubleType().noDefaultValue();
        } else if (type == Float.class) {
            return (ConfigOption<T>) optionBuilder.floatType().noDefaultValue();
        } else if (type == Boolean.class) {
            return (ConfigOption<T>) optionBuilder.booleanType().noDefaultValue();
        }

        throw new UnsupportedOperationException("Not supported type: " + type);
    }

    public static Object[] transform(RowData record, List<DataType> dataTypes) {
        Object[] values = new Object[record.getArity()];
        for (int i = 0; i < record.getArity(); ++i) {
            DataType dataType = dataTypes.get(i);
            values[i] = typeConversion(dataType.getLogicalType(), record, i);
        }
        return values;
    }

    /** Converts a decoded row to Flink's internal row representation. */
    public static GenericRowData toRowData(Object[] values, List<DataType> dataTypes) {
        if (values == null || dataTypes == null || values.length != dataTypes.size()) {
            throw new IllegalArgumentException("row size must match the Flink table schema");
        }
        GenericRowData rowData = new GenericRowData(dataTypes.size());
        for (int i = 0; i < dataTypes.size(); i++) {
            rowData.setField(i, toFlinkValue(values[i], dataTypes.get(i).getLogicalType()));
        }
        return rowData;
    }

    /** Converts a decoded field value to the internal type required by a Flink logical type. */
    public static Object toFlinkValue(Object value, LogicalType type) {
        if (value == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(value.toString());
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = value instanceof BigDecimal
                        ? (BigDecimal) value : new BigDecimal(value.toString());
                return DecimalData.fromBigDecimal(
                        decimal, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return toFlinkArray(value, (ArrayType) type);
            case MAP:
                return toFlinkMap(value, (MapType) type);
            case ROW:
                return toFlinkRow(value, (RowType) type);
            default:
                return value;
        }
    }

    private static GenericArrayData toFlinkArray(Object value, ArrayType type) {
        List<?> elements = (List<?>) value;
        Object[] converted = new Object[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            converted[i] = toFlinkValue(elements.get(i), type.getElementType());
        }
        return new GenericArrayData(converted);
    }

    private static GenericMapData toFlinkMap(Object value, MapType type) {
        Map<Object, Object> converted = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            converted.put(
                    toFlinkValue(entry.getKey(), type.getKeyType()),
                    toFlinkValue(entry.getValue(), type.getValueType()));
        }
        return new GenericMapData(converted);
    }

    private static GenericRowData toFlinkRow(Object value, RowType type) {
        Map<?, ?> fields = (Map<?, ?>) value;
        GenericRowData row = new GenericRowData(type.getFieldCount());
        for (int i = 0; i < type.getFieldCount(); i++) {
            RowType.RowField field = type.getFields().get(i);
            row.setField(i, toFlinkValue(fields.get(field.getName()), field.getType()));
        }
        return row;
    }

    public static Object typeConversion(LogicalType fieldType, RowData rowData, int index) {
        if (rowData.isNullAt(index)) {
            return null;
        }

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return rowData.getBoolean(index);
            case TINYINT:
                return rowData.getByte(index);
            case SMALLINT:
                return rowData.getShort(index);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return rowData.getInt(index);
            case BIGINT:
                return rowData.getLong(index);
            case FLOAT:
                return rowData.getFloat(index);
            case DOUBLE:
                return rowData.getDouble(index);
            case CHAR:
            case VARCHAR:
                return rowData.getString(index).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return rowData.getTimestamp(index, 3).toLocalDateTime();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();
            case BINARY:
            case VARBINARY:
                return rowData.getBinary(index);
            case ARRAY:
                return toJavaArray(rowData.getArray(index), ((ArrayType) fieldType).getElementType());
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    private static List<Object> toJavaArray(ArrayData array, LogicalType elementType) {
        List<Object> values = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            values.add(readArrayElement(array, elementType, i));
        }
        return values;
    }

    private static Object readArrayElement(ArrayData array, LogicalType type, int index) {
        if (array.isNullAt(index)) {
            return null;
        }
        if (type instanceof IntType) {
            return array.getInt(index);
        }
        if (type instanceof BigIntType) {
            return array.getLong(index);
        }
        if (type instanceof FloatType) {
            return array.getFloat(index);
        }
        if (type instanceof DoubleType) {
            return array.getDouble(index);
        }
        if (type instanceof VarCharType || type instanceof CharType) {
            return array.getString(index).toString();
        }
        throw new UnsupportedOperationException("Unsupported array element type: " + type);
    }
}
