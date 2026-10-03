package com.sqlrec.frontend.utils;

import com.sqlrec.common.utils.RowTransformUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;

public class ThriftUtils {
    private static final Logger logger = LoggerFactory.getLogger(ThriftUtils.class);

    public static TStatus errorStatus(Exception error) {
        TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
        String message = error.getMessage();
        status.setErrorMessage(message);
        status.setSqlState(error instanceof UnsupportedOperationException ? "0A000"
                : message != null && message.startsWith("FLINK_GATEWAY_UNAVAILABLE") ? "08001" : "HY000");
        return status;
    }

    public static boolean isSuccess(TStatus status) {
        return status != null && (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
                || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS);
    }

    public static TRowSet convertObjectArrayToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        TRowSet tRowSet = new TRowSet();
        List<TColumn> columns = new ArrayList<>();
        for (RelDataTypeField field : fields) {
            columns.add(toColumn(enumerable, field));
        }
        tRowSet.setRows(new ArrayList<>());
        tRowSet.setColumns(columns);
        tRowSet.setStartRowOffsetIsSet(true);
        return tRowSet;
    }

    private static TColumn toColumn(Enumerable<Object[]> rows, RelDataTypeField field) {
        int index = field.getIndex();
        return switch (field.getType().getSqlTypeName()) {
            case VARCHAR, CHAR -> valueColumn(rows, index, String.class,
                    (values, nulls) -> TColumn.stringVal(new TStringColumn(values, nulls)));
            case SMALLINT, TINYINT -> valueColumn(rows, index, Short.class,
                    (values, nulls) -> TColumn.i16Val(new TI16Column(values, nulls)));
            case INTEGER -> valueColumn(rows, index, Integer.class,
                    (values, nulls) -> TColumn.i32Val(new TI32Column(values, nulls)));
            case BIGINT -> valueColumn(rows, index, Long.class,
                    (values, nulls) -> TColumn.i64Val(new TI64Column(values, nulls)));
            case FLOAT, DOUBLE -> valueColumn(rows, index, Double.class,
                    (values, nulls) -> TColumn.doubleVal(new TDoubleColumn(values, nulls)));
            case BOOLEAN -> valueColumn(rows, index, Boolean.class,
                    (values, nulls) -> TColumn.boolVal(new TBoolColumn(values, nulls)));
            default -> valueColumn(RowTransformUtils.getJsonValueEnumerable(rows, index), 0, String.class,
                    (values, nulls) -> TColumn.stringVal(new TStringColumn(values, nulls)));
        };
    }

    private static <T> TColumn valueColumn(Enumerable<Object[]> rows, int index, Class<T> type,
            BiFunction<List<T>, ByteBuffer, TColumn> constructor) {
        Map.Entry<byte[], List<T>> values = getValueList(rows, index, type);
        return constructor.apply(values.getValue(), ByteBuffer.wrap(values.getKey()));
    }

    public static <T> Map.Entry<byte[], List<T>> getValueList(Enumerable<Object[]> enumerable, int index, Class<T> clazz) {
        if (enumerable == null) {
            return Map.entry(new byte[0], new ArrayList<>());
        }

        List<T> allDataList = new ArrayList<>();
        for (Object[] objects : enumerable) {
            Object object = objects[index];
            allDataList.add(tryCast(object, clazz));
        }

        byte[] nulls = new byte[(allDataList.size() + 7) / 8];
        for (int i = 0; i < allDataList.size(); i++) {
            if (allDataList.get(i) == null) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                nulls[byteIndex] |= (1 << bitIndex);
                allDataList.set(i, getDefaultValue(clazz));
            }
        }
        return Map.entry(nulls, allDataList);
    }

    public static <T> T tryCast(Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }

        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }

        if (Number.class.isAssignableFrom(clazz) && object instanceof Number) {
            Number num = (Number) object;
            if (clazz == Integer.class) {
                return (T) (Integer) num.intValue();
            } else if (clazz == Long.class) {
                return (T) (Long) num.longValue();
            } else if (clazz == Double.class) {
                return (T) (Double) num.doubleValue();
            } else if (clazz == Float.class) {
                return (T) (Float) num.floatValue();
            } else if (clazz == Short.class) {
                return (T) (Short) num.shortValue();
            } else if (clazz == Byte.class) {
                return (T) (Byte) num.byteValue();
            }
        }

        logger.warn("Failed to cast {} to {}", object.getClass().getName(), clazz.getName());
        return null;
    }

    public static <T> T getDefaultValue(Class<T> clazz) {
        if (clazz == Integer.class) {
            return (T) (Integer) 0;
        }
        if (clazz == Long.class) {
            return (T) (Long) 0L;
        }
        if (clazz == Double.class) {
            return (T) (Double) 0.0;
        }
        if (clazz == Float.class) {
            return (T) (Float) 0.0f;
        }
        if (clazz == Short.class) {
            return (T) (Short) (short) 0;
        }
        if (clazz == Byte.class) {
            return (T) (Byte) (byte) 0;
        }
        if (clazz == Boolean.class) {
            return (T) (Boolean) false;
        }
        if (clazz == String.class) {
            return (T) ("");
        }
        return null;
    }

    public static TTableSchema convertFieldsToTTableSchema(List<RelDataTypeField> fields) {
        TTableSchema schema = new TTableSchema();

        for (RelDataTypeField field : fields) {
            TPrimitiveTypeEntry typeEntry = new TPrimitiveTypeEntry(toTTypeId(field));
            typeEntry.setTypeQualifiers(new TTypeQualifiers(new HashMap<>()));
            TTypeDesc tTypeDesc = new TTypeDesc(
                    Collections.singletonList(TTypeEntry.primitiveEntry(typeEntry))
            );
            TColumnDesc columnDesc = new TColumnDesc(field.getName(), tTypeDesc, schema.getColumnsSize());
            schema.addToColumns(columnDesc);
        }
        return schema;
    }

    private static TTypeId toTTypeId(RelDataTypeField field) {
        return switch (field.getType().getSqlTypeName()) {
            case VARCHAR -> TTypeId.STRING_TYPE;
            case CHAR -> TTypeId.CHAR_TYPE;
            case SMALLINT -> TTypeId.SMALLINT_TYPE;
            case TINYINT -> TTypeId.TINYINT_TYPE;
            case INTEGER -> TTypeId.INT_TYPE;
            case BIGINT -> TTypeId.BIGINT_TYPE;
            case FLOAT -> TTypeId.FLOAT_TYPE;
            case DOUBLE -> TTypeId.DOUBLE_TYPE;
            case BOOLEAN -> TTypeId.BOOLEAN_TYPE;
            default -> TTypeId.STRING_TYPE;
        };
    }

    public static THandleIdentifier getHandleIdentifier() {
        byte[] guid = uuidToBytes(UUID.randomUUID());
        byte[] secret = uuidToBytes(UUID.randomUUID());
        return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
    }

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return bytes;
    }

    public static String safeHandleId(THandleIdentifier handleId) {
        if (handleId == null) {
            return "null";
        }
        byte[] guid = handleId.getGuid();
        if (guid == null) {
            return "guid=null";
        }
        return "guid=" + HexFormat.of().formatHex(guid);
    }

    public static String getQueryId() {
        return UUID.randomUUID().toString();
    }

}
