package com.sqlrec.frontend.utils;

import com.sqlrec.common.utils.JsonUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.hive.service.rpc.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;

public class ThriftUtils {
    private static final Logger logger = LoggerFactory.getLogger(ThriftUtils.class);
    private static final DateTimeFormatter TIMESTAMP_TEXT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.ROOT);

    private static final IntFunction<ResultColumn<?>> STRING_COLUMN =
            stringColumn(value -> tryCast(value, String.class));
    private static final IntFunction<ResultColumn<?>> DOUBLE_COLUMN = valueColumn(Double.class, 0.0,
            (values, nulls) -> TColumn.doubleVal(new TDoubleColumn(values, nulls)));
    private static final ColumnType FLOAT_COLUMN_TYPE = new ColumnType(TTypeId.FLOAT_TYPE, DOUBLE_COLUMN);
    private static final ColumnType JSON_COLUMN_TYPE =
            new ColumnType(TTypeId.STRING_TYPE, stringColumn(JsonUtils::toJson));
    private static final Map<SqlTypeName, ColumnType> COLUMN_TYPES = Map.ofEntries(
            Map.entry(SqlTypeName.VARCHAR, new ColumnType(TTypeId.STRING_TYPE, STRING_COLUMN)),
            Map.entry(SqlTypeName.CHAR, new ColumnType(TTypeId.CHAR_TYPE, STRING_COLUMN)),
            Map.entry(SqlTypeName.DATE, new ColumnType(TTypeId.DATE_TYPE, stringColumn(ThriftUtils::formatDate))),
            Map.entry(SqlTypeName.TIMESTAMP,
                    new ColumnType(TTypeId.TIMESTAMP_TYPE, stringColumn(ThriftUtils::formatTimestamp))),
            Map.entry(SqlTypeName.TINYINT, new ColumnType(TTypeId.TINYINT_TYPE, valueColumn(Byte.class, (byte) 0,
                    (values, nulls) -> TColumn.byteVal(new TByteColumn(values, nulls))))),
            Map.entry(SqlTypeName.SMALLINT, new ColumnType(TTypeId.SMALLINT_TYPE, valueColumn(Short.class, (short) 0,
                    (values, nulls) -> TColumn.i16Val(new TI16Column(values, nulls))))),
            Map.entry(SqlTypeName.INTEGER, new ColumnType(TTypeId.INT_TYPE, valueColumn(Integer.class, 0,
                    (values, nulls) -> TColumn.i32Val(new TI32Column(values, nulls))))),
            Map.entry(SqlTypeName.BIGINT, new ColumnType(TTypeId.BIGINT_TYPE, valueColumn(Long.class, 0L,
                    (values, nulls) -> TColumn.i64Val(new TI64Column(values, nulls))))),
            Map.entry(SqlTypeName.REAL, FLOAT_COLUMN_TYPE),
            Map.entry(SqlTypeName.FLOAT, FLOAT_COLUMN_TYPE),
            Map.entry(SqlTypeName.DOUBLE, new ColumnType(TTypeId.DOUBLE_TYPE, DOUBLE_COLUMN)),
            Map.entry(SqlTypeName.BOOLEAN, new ColumnType(TTypeId.BOOLEAN_TYPE, valueColumn(Boolean.class, false,
                    (values, nulls) -> TColumn.boolVal(new TBoolColumn(values, nulls))))));

    private record ColumnType(TTypeId typeId, IntFunction<ResultColumn<?>> columnFactory) {
    }

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
        List<ResultColumn<?>> resultColumns = new ArrayList<>(fields.size());
        for (RelDataTypeField field : fields) {
            resultColumns.add(columnType(field).columnFactory().apply(field.getIndex()));
        }
        if (enumerable != null && !resultColumns.isEmpty()) {
            try (Enumerator<Object[]> rows = enumerable.enumerator()) {
                while (rows.moveNext()) {
                    Object[] row = rows.current();
                    for (ResultColumn<?> column : resultColumns) {
                        column.add(row);
                    }
                }
            }
        }

        TRowSet tRowSet = new TRowSet();
        List<TColumn> columns = new ArrayList<>(fields.size());
        for (ResultColumn<?> column : resultColumns) {
            columns.add(column.finish());
        }
        tRowSet.setRows(new ArrayList<>());
        tRowSet.setColumns(columns);
        tRowSet.setStartRowOffsetIsSet(true);
        return tRowSet;
    }

    private static ColumnType columnType(RelDataTypeField field) {
        return COLUMN_TYPES.getOrDefault(field.getType().getSqlTypeName(), JSON_COLUMN_TYPE);
    }

    private static IntFunction<ResultColumn<?>> stringColumn(Function<Object, String> converter) {
        return index -> new ResultColumn<>(index, converter, "",
                (values, nulls) -> TColumn.stringVal(new TStringColumn(values, nulls)));
    }

    private static String formatDate(Object value) {
        if (value instanceof Number days) {
            return DateString.fromDaysSinceEpoch(days.intValue()).toString();
        }
        if (value instanceof Date date) {
            return date.toLocalDate().toString();
        }
        return LocalDate.parse(value.toString()).toString();
    }

    private static String formatTimestamp(Object value) {
        if (value instanceof Number millis) {
            // Calcite encodes the wall-clock value as epoch milliseconds.
            return TimestampString.fromMillisSinceEpoch(millis.longValue()).toString();
        }
        LocalDateTime timestamp = value instanceof Timestamp jdbcTimestamp
                ? jdbcTimestamp.toLocalDateTime()
                : LocalDateTime.parse(value.toString().replace(' ', 'T'));
        return TIMESTAMP_TEXT.format(timestamp);
    }

    private static <T> IntFunction<ResultColumn<?>> valueColumn(Class<T> type, T nullPlaceholder,
            BiFunction<List<T>, ByteBuffer, TColumn> constructor) {
        return index -> new ResultColumn<>(index, value -> tryCast(value, type), nullPlaceholder, constructor);
    }

    private static final class ResultColumn<T> {
        private final int index;
        private final Function<Object, T> convert;
        private final T nullPlaceholder;
        private final BiFunction<List<T>, ByteBuffer, TColumn> constructor;
        private final List<T> values = new ArrayList<>();
        private final BitSet nulls = new BitSet();

        private ResultColumn(int index, Function<Object, T> convert, T nullPlaceholder,
                BiFunction<List<T>, ByteBuffer, TColumn> constructor) {
            this.index = index;
            this.convert = convert;
            this.nullPlaceholder = nullPlaceholder;
            this.constructor = constructor;
        }

        private void add(Object[] row) {
            Object value = row[index];
            T converted = value == null ? null : convert.apply(value);
            if (converted == null) {
                nulls.set(values.size());
                converted = nullPlaceholder;
            }
            values.add(converted);
        }

        private TColumn finish() {
            byte[] bitmap = Arrays.copyOf(nulls.toByteArray(), (values.size() + 7) / 8);
            return constructor.apply(values, ByteBuffer.wrap(bitmap));
        }
    }

    private static <T> T tryCast(Object object, Class<T> clazz) {
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }

        if (object instanceof Number number) {
            if (clazz == Integer.class) {
                return clazz.cast(number.intValue());
            } else if (clazz == Long.class) {
                return clazz.cast(number.longValue());
            } else if (clazz == Double.class) {
                return clazz.cast(number.doubleValue());
            } else if (clazz == Short.class) {
                return clazz.cast(number.shortValue());
            } else if (clazz == Byte.class) {
                return clazz.cast(number.byteValue());
            }
        }

        logger.warn("Failed to cast {} to {}", object.getClass().getName(), clazz.getName());
        return null;
    }

    public static TTableSchema convertFieldsToTTableSchema(List<RelDataTypeField> fields) {
        TTableSchema schema = new TTableSchema();

        for (RelDataTypeField field : fields) {
            TPrimitiveTypeEntry typeEntry = new TPrimitiveTypeEntry(columnType(field).typeId());
            typeEntry.setTypeQualifiers(new TTypeQualifiers(new HashMap<>()));
            TTypeDesc tTypeDesc = new TTypeDesc(
                    Collections.singletonList(TTypeEntry.primitiveEntry(typeEntry))
            );
            TColumnDesc columnDesc = new TColumnDesc(field.getName(), tTypeDesc, schema.getColumnsSize());
            schema.addToColumns(columnDesc);
        }
        return schema;
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
