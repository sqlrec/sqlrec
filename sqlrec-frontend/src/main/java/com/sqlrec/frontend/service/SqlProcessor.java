package com.sqlrec.frontend.service;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.SqlCreateApi;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hive.service.rpc.thrift.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SqlProcessor {
    private CalciteSchema schema;
    private FunctionCompiler functionCompiler;
    private Map<THandleIdentifier, SqlProcessResult> sqlProcessorMap;

    public SqlProcessor() {
        schema = HmsSchema.getHmsCalciteSchema();
        sqlProcessorMap = new ConcurrentHashMap<>();
    }

    public SqlProcessResult getProcessProcessResult(THandleIdentifier handleIdentifier) {
        return sqlProcessorMap.getOrDefault(handleIdentifier, null);
    }

    public void closeProcessProcessResult(THandleIdentifier handleIdentifier) {
        sqlProcessorMap.remove(handleIdentifier);
    }

    public SqlProcessResult tryExecuteSql(String sql) throws Exception {
        SqlProcessResult result = executeSql(sql);
        if (result != null) {
            sqlProcessorMap.put(result.handleIdentifier, result);
        }
        return result;
    }

    private SqlProcessResult executeSql(String sql) throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        SqlProcessResult result = tryCompileFunction(sqlNode, sql);
        if (result != null) {
            return result;
        }

        if (sqlNode instanceof SqlCreateApi) {
            // todo save api
            return convertMsgToResult("create api success");
        }

        if (SqlTypeChecker.isFlinkSqlCompilable(sqlNode, schema)) {
            BindableInterface bindableInterface = CompileManager.compileSql(sqlNode, schema);
            Enumerable<Object[]> enumerable = bindableInterface.bind(schema);
            List<RelDataTypeField> fields = bindableInterface.getReturnDataFields();
            return convertEnumerableToTRowSet(enumerable, fields);
        }

        return null;
    }

    private SqlProcessResult tryCompileFunction(SqlNode sqlNode, String sql) {
        try {
            if (functionCompiler != null) {
                functionCompiler.compile(sqlNode, sql);
                if (functionCompiler.isFunctionCompileFinish()) {
                    functionCompiler = null;
                    // todo save function
                    return convertMsgToResult("function compile success");
                } else {
                    return convertMsgToResult("sql compile finish");
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null);
                functionCompiler.compile(sqlNode, sql);
                return convertMsgToResult("sql compile success");
            }
        } catch (Exception e) {
            functionCompiler = null;
            return convertMsgToResult("compile fcuntion error: " + e.getMessage());
        }

        return null;
    }

    public SqlProcessResult convertMsgToResult(String msg) {
        Enumerable<Object[]> enumerable = getMsgEnumerable(msg);
        List<RelDataTypeField> fields = getStringTypeFields("msg");
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), msg);
    }

    public SqlProcessResult convertEnumerableToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        return new SqlProcessResult(enumerable, fields, getHandleIdentifier(), getQueryId(), null);
    }

    public TRowSet convertObjectArrayToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
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

    public <T> List<T> getValueList(Enumerable<Object[]> enumerable, int index, Class<T> clazz) {
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

    public <T> T tryCast(Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
        return null;
    }

    public TTableSchema convertFieldsToTTableSchema(List<RelDataTypeField> fields) {
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

    public static List<RelDataTypeField> getStringTypeFields(String fieldName) {
        return Collections.singletonList(
                new RelDataTypeFieldImpl(
                        fieldName,
                        0,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)
                )
        );
    }

    public static Enumerable<Object[]> getMsgEnumerable(String msg) {
        if (msg == null) {
            return null;
        }
        return Linq4j.asEnumerable(Collections.singletonList(new String[]{msg}));
    }
}
