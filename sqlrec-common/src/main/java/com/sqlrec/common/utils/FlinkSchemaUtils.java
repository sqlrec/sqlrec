package com.sqlrec.common.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FlinkSchemaUtils {
    public static List<FieldSchema> getFieldSchemas(ResolvedSchema schema) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            fieldSchemas.add(new FieldSchema(col.getName(), col.getDataType().getLogicalType().getTypeRoot().name()));
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
        if (type == String.class) {
            return (ConfigOption<T>) optionBuilder.stringType().noDefaultValue();
        } else if (type == Integer.class) {
            return (ConfigOption<T>) optionBuilder.intType().noDefaultValue();
        } else if (type == Long.class) {
            return (ConfigOption<T>) optionBuilder.longType().noDefaultValue();
        } else if (type == Double.class) {
            return (ConfigOption<T>) optionBuilder.doubleType().noDefaultValue();
        } else if (type == Float.class) {
            return (ConfigOption<T>) optionBuilder.floatType().noDefaultValue();
        } else if (type == Short.class) {
            return (ConfigOption<T>) optionBuilder.intType().noDefaultValue();
        } else if (type == Byte.class) {
            return (ConfigOption<T>) optionBuilder.intType().noDefaultValue();
        } else if (type == Character.class) {
            return (ConfigOption<T>) optionBuilder.stringType().noDefaultValue();
        } else if (type == Boolean.class) {
            return (ConfigOption<T>) optionBuilder.booleanType().noDefaultValue();
        }

        throw new UnsupportedOperationException("Not supported type: " + type);
    }

    public static Object[] transform(RowData record, List<org.apache.flink.table.types.DataType> dataTypes) {
        Object[] values = new Object[record.getArity()];
        for (int i = 0; i < record.getArity(); ++i) {
            org.apache.flink.table.types.DataType dataType = dataTypes.get(i);
            values[i] = typeConvertion(dataType.getLogicalType(), record, i);
        }
        return values;
    }

    public static Object typeConvertion(LogicalType fieldType, RowData rowData, int index) {
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
            case DATE:
                return rowData.getInt(index);
            case TIME_WITHOUT_TIME_ZONE:
                return rowData.getInt(index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return rowData.getTimestamp(index, 3).toLocalDateTime();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();
            case BINARY:
            case VARBINARY:
                return rowData.getBinary(index);
            case ARRAY:
                if (isFloatVectorType(fieldType)) {
                    ArrayData arrayData = rowData.getArray(index);
                    List<Float> vector = new ArrayList<>((int) arrayData.size());
                    for (int i = 0; i < arrayData.size(); i++) {
                        vector.add(arrayData.getFloat(i));
                    }
                    return vector;
                }
                throw new UnsupportedOperationException("Unsupported array type: " + fieldType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    public static boolean isFloatVectorType(LogicalType fieldType) {
        if (fieldType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) fieldType;
            return arrayType.getElementType() instanceof FloatType;
        }
        return false;
    }
}