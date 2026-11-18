package com.sqlrec.common.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Encodes and decodes SQLRec rows with the schema of a generated Protobuf message class. */
public final class ProtobufRowCodec {
    private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger UINT32_MAX = BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE);
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger UINT64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private final Message defaultInstance;
    private final List<Column> columns;

    public ProtobufRowCodec(String messageClassName, List<FieldSchema> fieldSchemas) {
        if (messageClassName == null || messageClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("protobuf.message-class-name must not be empty");
        }
        this.defaultInstance = loadDefaultInstance(messageClassName);
        if (fieldSchemas == null) {
            throw new IllegalArgumentException("table schema must not be null");
        }
        Descriptors.Descriptor descriptor = defaultInstance.getDescriptorForType();
        this.columns = new ArrayList<>(fieldSchemas.size());
        for (FieldSchema schema : fieldSchemas) {
            String name = schema.getName();
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
            RelDataType type = field == null ? null : DataTypeUtils.getRelDataType(schema.getType());
            columns.add(new Column(name, field, type));
        }
    }

    public byte[] encode(Object[] row) {
        if (row == null) {
            throw new IllegalArgumentException("row must not be null");
        }
        if (row.length != columns.size()) {
            throw new IllegalArgumentException("row size must match the table schema");
        }

        Message.Builder builder = defaultInstance.newBuilderForType();
        Descriptors.Descriptor descriptor = defaultInstance.getDescriptorForType();
        for (int i = 0; i < row.length; i++) {
            Object value = row[i];
            if (value != null) {
                setField(builder, columns.get(i).requireField(descriptor), value);
            }
        }
        return builder.build().toByteArray();
    }

    public Object[] decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }

        Message message;
        try {
            message = defaultInstance.getParserForType().parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Unable to parse " + defaultInstance.getDescriptorForType().getFullName(), e);
        }

        Object[] row = new Object[columns.size()];
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        for (int i = 0; i < row.length; i++) {
            Column column = columns.get(i);
            row[i] = readField(message, column.requireField(descriptor), column.sqlType);
        }
        return row;
    }

    private static final class Column {
        private final String name;
        private final Descriptors.FieldDescriptor field;
        private final RelDataType sqlType;

        private Column(String name, Descriptors.FieldDescriptor field, RelDataType sqlType) {
            this.name = name;
            this.field = field;
            this.sqlType = sqlType;
        }

        private Descriptors.FieldDescriptor requireField(Descriptors.Descriptor descriptor) {
            if (field == null) {
                throw new IllegalArgumentException(
                        "Table field '" + name + "' does not exist in Protobuf message "
                                + descriptor.getFullName());
            }
            return field;
        }
    }

    private static Message loadDefaultInstance(String messageClassName) {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader == null) {
                contextClassLoader = ProtobufRowCodec.class.getClassLoader();
            }
            Class<?> messageClass = Class.forName(messageClassName, true, contextClassLoader);
            Method method = messageClass.getMethod("getDefaultInstance");
            Object instance = method.invoke(null);
            if (!(instance instanceof Message)) {
                throw new IllegalArgumentException(
                        messageClassName + " is not a generated Protobuf Message class");
            }
            return (Message) instance;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException(
                    "Unable to load Protobuf message class " + messageClassName, e);
        }
    }

    private static void setField(
            Message.Builder builder,
            Descriptors.FieldDescriptor field,
            Object value
    ) {
        if (field.isMapField()) {
            if (!(value instanceof Map)) {
                throw incompatible(field, value);
            }
            Descriptors.Descriptor entryType = field.getMessageType();
            Descriptors.FieldDescriptor keyField = entryType.findFieldByName("key");
            Descriptors.FieldDescriptor valueField = entryType.findFieldByName("value");
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                DynamicMessage.Builder entryBuilder = DynamicMessage.newBuilder(entryType);
                entryBuilder.setField(keyField, convertScalar(keyField, entry.getKey()));
                entryBuilder.setField(valueField, convertScalar(valueField, entry.getValue()));
                builder.addRepeatedField(field, entryBuilder.build());
            }
            return;
        }

        if (field.isRepeated()) {
            if (value instanceof Iterable) {
                for (Object element : (Iterable<?>) value) {
                    builder.addRepeatedField(field, convertScalar(field, element));
                }
                return;
            }
            if (value.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(value); i++) {
                    builder.addRepeatedField(field, convertScalar(field, Array.get(value, i)));
                }
                return;
            }
            throw incompatible(field, value);
        }

        builder.setField(field, convertScalar(field, value));
    }

    private static Object convertScalar(Descriptors.FieldDescriptor field, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Protobuf repeated and map values cannot contain null for field "
                            + field.getFullName());
        }
        try {
            switch (field.getJavaType()) {
                case INT:
                case LONG:
                    return convertInteger(field, value);
                case FLOAT:
                    return ((Number) value).floatValue();
                case DOUBLE:
                    return ((Number) value).doubleValue();
                case BOOLEAN:
                    return (Boolean) value;
                case STRING:
                    return value.toString();
                case BYTE_STRING:
                    return convertBytes(field, value);
                case ENUM:
                    return convertEnum(field, value);
                case MESSAGE:
                    return convertMessage(field, value);
                default:
                    throw incompatible(field, value);
            }
        } catch (ClassCastException e) {
            throw incompatible(field, value);
        }
    }

    private static ByteString convertBytes(Descriptors.FieldDescriptor field, Object value) {
        if (value instanceof ByteString) {
            return (ByteString) value;
        }
        if (value instanceof byte[]) {
            return ByteString.copyFrom((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            return ByteString.copyFrom(((ByteBuffer) value).duplicate());
        }
        throw incompatible(field, value);
    }

    private static Descriptors.EnumValueDescriptor convertEnum(Descriptors.FieldDescriptor field, Object value) {
        Descriptors.EnumValueDescriptor enumValue;
        if (value instanceof Number) {
            BigInteger number = exactInteger(field, (Number) value);
            if (number.compareTo(INT32_MIN) < 0 || number.compareTo(INT32_MAX) > 0) {
                throw new IllegalArgumentException(
                        "Integer value " + number + " is out of range for Protobuf field " + field.getFullName());
            }
            enumValue = field.getEnumType().findValueByNumber(number.intValue());
        } else {
            enumValue = field.getEnumType().findValueByName(value.toString());
        }
        if (enumValue == null) {
            throw new IllegalArgumentException(
                    "Unknown enum value '" + value + "' for field " + field.getFullName());
        }
        return enumValue;
    }

    private static Message convertMessage(Descriptors.FieldDescriptor field, Object value) {
        if (value instanceof Message
                && ((Message) value).getDescriptorForType().equals(field.getMessageType())) {
            return (Message) value;
        }
        if (!(value instanceof Map)) {
            throw incompatible(field, value);
        }
        DynamicMessage.Builder nested = DynamicMessage.newBuilder(field.getMessageType());
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            Descriptors.FieldDescriptor nestedField = field.getMessageType()
                    .findFieldByName(String.valueOf(entry.getKey()));
            if (nestedField == null) {
                throw new IllegalArgumentException(
                        "Unknown nested field '" + entry.getKey() + "' for " + field.getMessageType().getFullName());
            }
            setField(nested, nestedField, entry.getValue());
        }
        return nested.build();
    }

    private static Object convertInteger(Descriptors.FieldDescriptor field, Object value) {
        if (!(value instanceof Number)) {
            throw incompatible(field, value);
        }
        BigInteger integer = exactInteger(field, (Number) value);
        final BigInteger min;
        final BigInteger max;
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                min = INT32_MIN;
                max = INT32_MAX;
                break;
            case UINT32:
            case FIXED32:
                min = BigInteger.ZERO;
                max = UINT32_MAX;
                break;
            case INT64:
            case SINT64:
            case SFIXED64:
                min = INT64_MIN;
                max = INT64_MAX;
                break;
            case UINT64:
            case FIXED64:
                min = BigInteger.ZERO;
                max = UINT64_MAX;
                break;
            default:
                throw incompatible(field, value);
        }
        if (integer.compareTo(min) < 0 || integer.compareTo(max) > 0) {
            throw new IllegalArgumentException(
                    "Integer value " + integer + " is out of range for Protobuf field "
                            + field.getFullName());
        }
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
            return integer.intValue();
        }
        return integer.longValue();
    }

    private static BigInteger exactInteger(Descriptors.FieldDescriptor field, Number value) {
        try {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toBigIntegerExact();
            }
            if (value instanceof Byte || value instanceof Short
                    || value instanceof Integer || value instanceof Long) {
                return BigInteger.valueOf(value.longValue());
            }
            return new BigDecimal(value.toString()).toBigIntegerExact();
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Expected an exact integer for Protobuf field " + field.getFullName(), e);
        }
    }

    private static Object readField(
            Message message,
            Descriptors.FieldDescriptor field,
            RelDataType sqlType
    ) {
        if (field.isMapField()) {
            Map<Object, Object> result = new LinkedHashMap<>();
            Descriptors.FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
            RelDataType keyType = sqlType == null ? null : sqlType.getKeyType();
            RelDataType valueType = sqlType == null ? null : sqlType.getValueType();
            for (Object value : (List<?>) message.getField(field)) {
                Message entry = (Message) value;
                result.put(
                        fromProtobufValue(keyField, entry.getField(keyField), keyType),
                        fromProtobufValue(valueField, entry.getField(valueField), valueType));
            }
            return result;
        }

        if (field.isRepeated()) {
            List<?> values = (List<?>) message.getField(field);
            List<Object> result = new ArrayList<>(values.size());
            RelDataType elementType = sqlType == null ? null : sqlType.getComponentType();
            for (Object value : values) {
                result.add(fromProtobufValue(field, value, elementType));
            }
            return result;
        }

        if (field.hasPresence() && !message.hasField(field)) {
            return null;
        }
        return fromProtobufValue(field, message.getField(field), sqlType);
    }

    private static Object fromProtobufValue(
            Descriptors.FieldDescriptor field,
            Object value,
            RelDataType sqlType
    ) {
        SqlTypeName target = sqlType == null ? null : sqlType.getSqlTypeName();
        switch (field.getJavaType()) {
            case INT:
                return fromProtobufInt(field, (Integer) value, target);
            case LONG:
                return fromProtobufLong(field, (Long) value, target);
            case BYTE_STRING:
                return ((ByteString) value).toByteArray();
            case ENUM:
                Descriptors.EnumValueDescriptor enumValue =
                        (Descriptors.EnumValueDescriptor) value;
                return target == SqlTypeName.CHAR || target == SqlTypeName.VARCHAR
                        ? enumValue.getName() : enumValue.getNumber();
            case MESSAGE:
                Message nestedMessage = (Message) value;
                Map<String, Object> nested = new LinkedHashMap<>();
                for (Descriptors.FieldDescriptor nestedField
                        : nestedMessage.getDescriptorForType().getFields()) {
                    nested.put(
                            nestedField.getName(),
                            readField(nestedMessage, nestedField, null));
                }
                return nested;
            default:
                return value;
        }
    }

    private static Object fromProtobufInt(
            Descriptors.FieldDescriptor field, int value, SqlTypeName sqlType) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.UINT32
                || field.getType() == Descriptors.FieldDescriptor.Type.FIXED32) {
            long unsigned = Integer.toUnsignedLong(value);
            if (sqlType == SqlTypeName.DECIMAL) {
                return BigDecimal.valueOf(unsigned);
            }
            if (sqlType == SqlTypeName.BIGINT) {
                return unsigned;
            }
            if (unsigned > Integer.MAX_VALUE) {
                if (sqlType != null) {
                    throw new IllegalArgumentException(
                            "Protobuf field " + field.getFullName()
                                    + " requires a BIGINT or DECIMAL SQL column for value " + unsigned);
                }
                return unsigned;
            }
        }
        if (sqlType == SqlTypeName.BIGINT) {
            return (long) value;
        }
        return value;
    }

    private static Object fromProtobufLong(
            Descriptors.FieldDescriptor field, long value, SqlTypeName sqlType) {
        if ((field.getType() == Descriptors.FieldDescriptor.Type.UINT64
                || field.getType() == Descriptors.FieldDescriptor.Type.FIXED64)
                && value < 0) {
            BigInteger unsigned = new BigInteger(Long.toUnsignedString(value));
            if (sqlType == SqlTypeName.DECIMAL) {
                return new BigDecimal(unsigned);
            }
            if (sqlType != null) {
                throw new IllegalArgumentException(
                        "Protobuf field " + field.getFullName()
                                + " requires a DECIMAL SQL column for value " + unsigned);
            }
            return unsigned;
        }
        return sqlType == SqlTypeName.DECIMAL ? BigDecimal.valueOf(value) : value;
    }

    private static IllegalArgumentException incompatible(
            Descriptors.FieldDescriptor field,
            Object value
    ) {
        return new IllegalArgumentException(
                "Value of type " + value.getClass().getName()
                        + " is incompatible with Protobuf field " + field.getFullName());
    }
}
