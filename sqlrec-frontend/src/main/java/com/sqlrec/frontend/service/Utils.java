package com.sqlrec.frontend.service;

import com.google.gson.Gson;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.frontend.common.SqlProcessResult;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {
    public static SqlProcessResult convertMsgToResult(String msg, String fieldName) {
        Enumerable<Object[]> enumerable = getMsgEnumerable(msg);
        List<RelDataTypeField> fields = getStringTypeField(fieldName);
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), null);
    }

    public static SqlProcessResult convertStringListToResult(List<String> list, String fieldName) {
        Enumerable<Object[]> enumerable = convertListToEnumerable(list);
        List<RelDataTypeField> fields = getStringTypeField(fieldName);
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), null);
    }

    public static SqlProcessResult convertEnumerableToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), null);
    }

    public static SqlProcessResult getTableTypeDescResult(List<RelDataTypeField> dataFields) {
        List<List<String>> fieldNameAndType = dataFields.stream().map(
                f -> Arrays.asList(f.getName(), f.getType().toString())
        ).collect(Collectors.toList());
        Enumerable<Object[]> enumerable = convertListToArrayToEnumerable(fieldNameAndType);
        List<RelDataTypeField> resultFields = getStringTypeFieldList(
                Arrays.asList("name", "type")
        );
        return new SqlProcessResult(enumerable, resultFields, getHandleIdentifier(), getQueryId(), null);
    }

    public static TRowSet convertObjectArrayToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        TRowSet tRowSet = new TRowSet();
        List<TColumn> columns = new ArrayList<>();
        for (RelDataTypeField field : fields) {
            TColumn column = new TColumn();
            columns.add(column);
            switch (field.getType().getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    TStringColumn stringColumn = new TStringColumn();
                    Map.Entry<byte[], List<String>> charEntry = getValueList(enumerable, field.getIndex(), String.class);
                    stringColumn.setValues(charEntry.getValue());
                    stringColumn.setNulls(charEntry.getKey());
                    column.setStringVal(stringColumn);
                    break;
                case SMALLINT:
                case TINYINT:
                    TI16Column i16Column = new TI16Column();
                    Map.Entry<byte[], List<Short>> shortEntry = getValueList(enumerable, field.getIndex(), Short.class);
                    i16Column.setValues(shortEntry.getValue());
                    i16Column.setNulls(shortEntry.getKey());
                    column.setI16Val(i16Column);
                    break;
                case INTEGER:
                    TI32Column i32Column = new TI32Column();
                    Map.Entry<byte[], List<Integer>> intEntry = getValueList(enumerable, field.getIndex(), Integer.class);
                    i32Column.setValues(intEntry.getValue());
                    i32Column.setNulls(intEntry.getKey());
                    column.setI32Val(i32Column);
                    break;
                case BIGINT:
                    TI64Column i64Column = new TI64Column();
                    Map.Entry<byte[], List<Long>> longEntry = getValueList(enumerable, field.getIndex(), Long.class);
                    i64Column.setValues(longEntry.getValue());
                    i64Column.setNulls(longEntry.getKey());
                    column.setI64Val(i64Column);
                    break;
                case FLOAT:
                case DOUBLE:
                    TDoubleColumn doubleColumn = new TDoubleColumn();
                    Map.Entry<byte[], List<Double>> doubleEntry = getValueList(enumerable, field.getIndex(), Double.class);
                    doubleColumn.setValues(doubleEntry.getValue());
                    doubleColumn.setNulls(doubleEntry.getKey());
                    column.setDoubleVal(doubleColumn);
                    break;
                case BOOLEAN:
                    TBoolColumn booleanColumn = new TBoolColumn();
                    Map.Entry<byte[], List<Boolean>> boolEntry = getValueList(enumerable, field.getIndex(), Boolean.class);
                    booleanColumn.setValues(boolEntry.getValue());
                    booleanColumn.setNulls(boolEntry.getKey());
                    column.setBoolVal(booleanColumn);
                    break;
                default:
                    TStringColumn defaultStringColumn = new TStringColumn();
                    defaultStringColumn.setValues(getJsonValueList(enumerable, field.getIndex()));
                    defaultStringColumn.setNulls(new byte[]{});
                    column.setStringVal(defaultStringColumn);
            }
        }
        tRowSet.setRows(new ArrayList<>());
        tRowSet.setColumns(columns);
        tRowSet.setStartRowOffsetIsSet(true);
        return tRowSet;
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

        List<T> retList = new ArrayList<>();
        byte[] nulls = new byte[(allDataList.size() + 7) / 8];
        for (int i = 0; i < allDataList.size(); i++) {
            if (allDataList.get(i) == null) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                nulls[byteIndex] |= (1 << bitIndex);
                retList.add(getDefaultValue(clazz));
            } else {
                retList.add(allDataList.get(i));
            }
        }
        return Map.entry(nulls, retList);
    }

    public static List<String> getJsonValueList(Enumerable<Object[]> enumerable, int index) {
        List<String> list = new ArrayList<>();
        if (enumerable == null) {
            return list;
        }

        for (Object[] objects : enumerable) {
            Object object = objects[index];
            list.add(new Gson().toJson(object));
        }
        return list;
    }

    public static <T> T tryCast(Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
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
            TTypeId tTypeId = null;
            switch (field.getType().getSqlTypeName()) {
                case VARCHAR:
                    tTypeId = TTypeId.STRING_TYPE;
                    break;
                case CHAR:
                    tTypeId = TTypeId.CHAR_TYPE;
                    break;
                case SMALLINT:
                    tTypeId = TTypeId.SMALLINT_TYPE;
                    break;
                case TINYINT:
                    tTypeId = TTypeId.TINYINT_TYPE;
                    break;
                case INTEGER:
                    tTypeId = TTypeId.INT_TYPE;
                    break;
                case BIGINT:
                    tTypeId = TTypeId.BIGINT_TYPE;
                    break;
                case FLOAT:
                    tTypeId = TTypeId.FLOAT_TYPE;
                    break;
                case DOUBLE:
                    tTypeId = TTypeId.DOUBLE_TYPE;
                    break;
                case BOOLEAN:
                    tTypeId = TTypeId.BOOLEAN_TYPE;
                    break;
                default:
                    tTypeId = TTypeId.STRING_TYPE;
            }

            TPrimitiveTypeEntry typeEntry = new TPrimitiveTypeEntry(tTypeId);
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
        UUID publicId = UUID.randomUUID();
        UUID secretId = UUID.randomUUID();

        byte[] guid = new byte[16];
        ByteBuffer pbb = ByteBuffer.wrap(guid);

        byte[] secret = new byte[16];
        ByteBuffer sbb = ByteBuffer.wrap(secret);

        pbb.putLong(publicId.getMostSignificantBits());
        pbb.putLong(publicId.getLeastSignificantBits());

        sbb.putLong(secretId.getMostSignificantBits());
        sbb.putLong(secretId.getLeastSignificantBits());

        return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
    }

    public static String getQueryId() {
        return UUID.randomUUID().toString();
    }

    public static List<RelDataTypeField> getStringTypeField(String fieldName) {
        return getStringTypeFieldList(Collections.singletonList(fieldName));
    }

    public static List<RelDataTypeField> getStringTypeFieldList(List<String> fieldName) {
        List<RelDataTypeField> fields = new ArrayList<>();
        int index = 0;
        for (String name : fieldName) {
            fields.add(DataTypeUtils.getRelDataTypeField(name, index++, SqlTypeName.VARCHAR));
        }
        return fields;
    }

    public static Enumerable<Object[]> getMsgEnumerable(String msg) {
        if (msg == null) {
            return null;
        }
        return Linq4j.asEnumerable(Collections.singletonList(new String[]{msg}));
    }

    public static Enumerable<Object[]> convertListToEnumerable(List<String> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(o -> new String[]{o}).collect(Collectors.toList()));
    }

    public static <T> Enumerable<Object[]> convertListToArrayToEnumerable(List<List<T>> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(List::toArray).collect(Collectors.toList()));
    }
}
