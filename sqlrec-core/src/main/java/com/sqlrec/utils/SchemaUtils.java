package com.sqlrec.utils;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SchemaUtils {
    private static final Logger log = LoggerFactory.getLogger(SchemaUtils.class);

    public static void addFunction(CalciteSchema schema, String functionName, String className) throws Exception {
        schema.plus().add(
                functionName, Objects.requireNonNull(createScalarFunction(className))
        );
    }

    public static ScalarFunction createScalarFunction(String className) throws Exception {
        return createScalarFunction(className, "eval");
    }

    public static ScalarFunction createScalarFunction(String className, String methodName) throws Exception {
        Class<?> clazz = Class.forName(className);
        return ScalarFunctionImpl.create(clazz, methodName);
    }

    public static String getValueOfStringLiteral(SqlCharStringLiteral value) {
        if (value == null) {
            return null;
        }
        String valueStr = value.toString();
        return removeQuotes(valueStr);
    }

    public static String getValueOfStringLiteral(SqlNode value) {
        if (value == null) {
            return null;
        }
        if (value instanceof SqlCharStringLiteral) {
            return getValueOfStringLiteral((SqlCharStringLiteral) value);
        }
        throw new RuntimeException("value is not a string literal: " + value);
    }

    public static String removeQuotes(String value) {
        if (value == null) {
            return null;
        }
        if ((value.startsWith("'") && value.endsWith("'")) ||
                (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static List<FieldSchema> convertFieldList(SqlNodeList fieldList) {
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        if (fieldList != null && fieldList.size() > 0) {
            for (SqlNode field : fieldList) {
                FieldSchema fieldSchema = convertField(field);
                if (fieldSchema != null) {
                    fieldSchemas.add(fieldSchema);
                }
            }
        }
        return fieldSchemas;
    }

    public static FieldSchema convertField(SqlNode field) {
        if (field == null) {
            return null;
        }

        if (field instanceof SqlTableColumn.SqlRegularColumn) {
            SqlTableColumn.SqlRegularColumn regularColumn = (SqlTableColumn.SqlRegularColumn) field;
            String name = regularColumn.getName().toString();
            String type = regularColumn.getType().toString();
            return new FieldSchema(name, type);
        } else {
            log.warn("Unsupported field type: {}", field);
            throw new IllegalArgumentException("Unsupported field type: " + field);
        }
    }

    public static Map<String, String> convertPropertyList(SqlNodeList propertyList) {
        Map<String, String> params = new HashMap<>();
        if (propertyList != null && propertyList.size() > 0) {
            for (SqlNode property : propertyList) {
                if (property instanceof SqlTableOption) {
                    SqlTableOption option = (SqlTableOption) property;
                    String key = removeQuotes(option.getKey().toString());
                    String value = removeQuotes(option.getValue().toString());
                    params.put(key, value);
                } else {
                    log.warn("Unsupported property type: {}", property);
                    throw new IllegalArgumentException("Unsupported property type: " + property);
                }
            }
        }
        return params;
    }

    public static SqlNodeList addConfigToPropertyList(SqlNodeList propertyList, String key, String value) {
        SqlTableOption option = new SqlTableOption(
                SqlLiteral.createCharString(key, SqlParserPos.ZERO),
                SqlLiteral.createCharString(value, SqlParserPos.ZERO),
                SqlParserPos.ZERO
        );
        propertyList.add(option);
        return propertyList;
    }

    public static List<RelDataTypeField> getDataTypeByLikeTableName(
            String likeTableName,
            CalciteSchema schema) {
        CalciteSchema.TableEntry tableEntry = schema.getTable(likeTableName, false);
        if (tableEntry == null) {
            throw new RuntimeException("like table not found: " + likeTableName);
        }
        Table table = tableEntry.getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("like table must be cache table for table function");
        }
        return ((CacheTable) table).getDataFields();
    }

    public static CacheTable getCacheTable(String inputTableName, CalciteSchema schema) {
        Table table = Objects.requireNonNull(
                schema.getTable(inputTableName, false),
                "input table not found: " + inputTableName
        ).getTable();
        if (!(table instanceof CacheTable)) {
            throw new RuntimeException("input table must be cache table for table function");
        }
        return (CacheTable) table;
    }

    public static Table getTableObj(CalciteSchema schema, String shortTableName) {
        if (schema == null || shortTableName == null) {
            return null;
        }
        CalciteSchema.TableEntry tableEntry = schema.getTable(shortTableName, false);
        if (tableEntry == null) {
            return null;
        }
        return tableEntry.getTable();
    }
}
