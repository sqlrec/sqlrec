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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.hive.service.rpc.thrift.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlProcessor {
    private CalciteSchema schema;
    private FunctionCompiler functionCompiler;

    public SqlProcessor() {
        schema = HmsSchema.getHmsCalciteSchema();
    }

    public TRowSet tryExecuteSql(String sql) throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        TRowSet rowSet = tryCompileFunction(sqlNode, sql);
        if (rowSet != null) {
            return rowSet;
        }

        if (sqlNode instanceof SqlCreateApi) {
            // todo save api
            return convertMsgToTRowSet("create api success");
        }

        if (SqlTypeChecker.isFlinkSqlCompilable(sqlNode, schema)) {
            BindableInterface bindableInterface = CompileManager.compileSql(sqlNode, schema);
            Enumerable<Object[]> enumerable = bindableInterface.bind(schema);
            List<RelDataTypeField> fields = bindableInterface.getReturnDataFields();
            return convertEnumerableToTRowSet(enumerable, fields);
        }

        return null;
    }

    private TRowSet tryCompileFunction(SqlNode sqlNode, String sql) {
        try {
            if (functionCompiler != null) {
                functionCompiler.compile(sqlNode, sql);
                if (functionCompiler.isFunctionCompileFinish()) {
                    functionCompiler = null;
                    // todo save function
                    return convertMsgToTRowSet("function compile success");
                } else {
                    return convertMsgToTRowSet("sql compile finish");
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null);
                functionCompiler.compile(sqlNode, sql);
                return convertMsgToTRowSet("sql compile success");
            }
        } catch (Exception e) {
            return convertMsgToTRowSet("compile fcuntion error: " + e.getMessage());
        }

        return null;
    }

    public TRowSet convertMsgToTRowSet(String msg) {
        TRowSet tRowSet = new TRowSet();

        TRow tRow = new TRow();
        TColumnValue tColumn = new TColumnValue();
        TStringValue tStringValue = new TStringValue();
        tStringValue.setValue(msg);
        tColumn.setFieldValue(TColumnValue._Fields.STRING_VAL, tStringValue);
        tRow.addToColVals(tColumn);
        tRowSet.addToRows(tRow);
        tRowSet.setColumnCount(1);
        return tRowSet;
    }

    public TRowSet convertEnumerableToTRowSet(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        TRowSet tRowSet = new TRowSet();
        List<TRow> rows = new ArrayList<>();
        List<TColumn> columns = new ArrayList<>();

        if (fields == null || fields.isEmpty()) {
            tRowSet.setRows(rows);
            return tRowSet;
        }

        int fieldCount = fields.size();
        Iterator<Object[]> iterator = enumerable.iterator();
        while (iterator.hasNext()) {
            Object[] rowData = iterator.next();
            TRow tRow = new TRow();

            for (int i = 0; i < fieldCount; i++) {
                Object value = rowData[i];
                TColumnValue tColumn = convertValueToTColumn(value, fields.get(i));
                tRow.addToColVals(tColumn);
            }

            rows.add(tRow);
        }

        tRowSet.setRows(rows);
        tRowSet.setColumns(columns);
        tRowSet.setColumnCount(fields.size());
        return tRowSet;
    }

    private TColumnValue convertValueToTColumn(Object value, RelDataTypeField field) {
        TColumnValue tColumn = new TColumnValue();

        if (value == null) {
            return tColumn;
        }

        switch (field.getType().getSqlTypeName()) {
            case CHAR:
            case VARCHAR:
                TStringValue tStringValue = new TStringValue();
                tStringValue.setValue(value.toString());
                tColumn.setFieldValue(TColumnValue._Fields.STRING_VAL, tStringValue);
                break;
            case BOOLEAN:
                TBoolValue tBooleanValue = new TBoolValue();
                tBooleanValue.setValue((Boolean) value);
                tColumn.setFieldValue(TColumnValue._Fields.BOOL_VAL, tBooleanValue);
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                TI32Value tIntValue = new TI32Value();
                tIntValue.setValue((Integer) value);
                tColumn.setFieldValue(TColumnValue._Fields.I32_VAL, tIntValue);
                break;
            case BIGINT:
                TI64Value tLongValue = new TI64Value();
                tLongValue.setValue((Long) value);
                tColumn.setFieldValue(TColumnValue._Fields.I64_VAL, tLongValue);
                break;
            case FLOAT:
            case DOUBLE:
                TDoubleValue tDoubleValue = new TDoubleValue();
                tDoubleValue.setValue((Double) value);
                tColumn.setFieldValue(TColumnValue._Fields.DOUBLE_VAL, tDoubleValue);
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                tStringValue = new TStringValue();
                tStringValue.setValue(value.toString());
                tColumn.setFieldValue(TColumnValue._Fields.STRING_VAL, tStringValue);
                break;
            default:
                tStringValue = new TStringValue();
                tStringValue.setValue(value.toString());
                tColumn.setFieldValue(TColumnValue._Fields.STRING_VAL, tStringValue);
        }

        return tColumn;
    }
}
