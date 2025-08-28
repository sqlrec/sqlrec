package com.sqlrec.frontend.service;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {
    public static SqlProcessResult convertMsgToResult(String msg) {
        Enumerable<Object[]> enumerable = getMsgEnumerable(msg);
        List<RelDataTypeField> fields = getStringTypeField("msg");
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), msg);
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
                    stringColumn.setValues(getValueList(enumerable, field.getIndex(), String.class));
                    stringColumn.setNulls(new byte[]{});
                    column.setStringVal(stringColumn);
                    break;
                case SMALLINT:
                case TINYINT:
                    TI16Column i16Column = new TI16Column();
                    i16Column.setValues(getValueList(enumerable, field.getIndex(), Short.class));
                    i16Column.setNulls(new byte[]{});
                    column.setI16Val(i16Column);
                    break;
                case INTEGER:
                    TI32Column i32Column = new TI32Column();
                    i32Column.setValues(getValueList(enumerable, field.getIndex(), Integer.class));
                    i32Column.setNulls(new byte[]{});
                    column.setI32Val(i32Column);
                    break;
                case BIGINT:
                    TI64Column i64Column = new TI64Column();
                    i64Column.setValues(getValueList(enumerable, field.getIndex(), Long.class));
                    i64Column.setNulls(new byte[]{});
                    column.setI64Val(i64Column);
                    break;
                case FLOAT:
                case DOUBLE:
                    TDoubleColumn doubleColumn = new TDoubleColumn();
                    doubleColumn.setValues(getValueList(enumerable, field.getIndex(), Double.class));
                    doubleColumn.setNulls(new byte[]{});
                    column.setDoubleVal(doubleColumn);
                    break;
                case BOOLEAN:
                    TBoolColumn booleanColumn = new TBoolColumn();
                    booleanColumn.setValues(getValueList(enumerable, field.getIndex(), Boolean.class));
                    booleanColumn.setNulls(new byte[]{});
                    column.setBoolVal(booleanColumn);
                    break;
                default:
                    throw new RuntimeException("not support type: " + field.getType().getSqlTypeName());
            }
        }
        tRowSet.setRows(new ArrayList<>());
        tRowSet.setColumns(columns);
        tRowSet.setStartRowOffsetIsSet(true);
        return tRowSet;
    }

    public static <T> List<T> getValueList(Enumerable<Object[]> enumerable, int index, Class<T> clazz) {
        List<T> list = new ArrayList<>();
        if (enumerable == null) {
            return list;
        }

        for (Object[] objects : enumerable) {
            Object object = objects[index];
            list.add(tryCast(object, clazz));
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
                    throw new RuntimeException("not support type: " + field.getType().getSqlTypeName());
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
            fields.add(new RelDataTypeFieldImpl(
                    name,
                    index++,
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)
            ));
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
