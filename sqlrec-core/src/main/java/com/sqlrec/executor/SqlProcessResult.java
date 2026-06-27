package com.sqlrec.executor;

import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SqlProcessResult {
    private Enumerable<Object[]> enumerable;
    private List<RelDataTypeField> fields;

    public SqlProcessResult() {
    }

    public SqlProcessResult(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        this.enumerable = enumerable;
        this.fields = fields;
    }

    public static SqlProcessResult msg(String msg, String fieldName) {
        return new SqlProcessResult(DataTransformUtils.getMsgEnumerable(msg), DataTypeUtils.getStringTypeField(fieldName));
    }

    public static SqlProcessResult of(Enumerable<Object[]> enumerable, List<RelDataTypeField> fields) {
        return new SqlProcessResult(enumerable, fields);
    }

    public static SqlProcessResult stringList(List<String> list, String fieldName) {
        return new SqlProcessResult(DataTransformUtils.convertListToEnumerable(list), DataTypeUtils.getStringTypeField(fieldName));
    }

    public static SqlProcessResult tableTypeDesc(List<RelDataTypeField> dataFields) {
        List<List<String>> fieldNameAndType = dataFields.stream().map(
                f -> Arrays.asList(f.getName(), f.getType().toString())
        ).collect(Collectors.toList());
        Enumerable<Object[]> enumerable = DataTransformUtils.convertListToArrayToEnumerable(fieldNameAndType);
        List<RelDataTypeField> resultFields = DataTypeUtils.getStringTypeFieldList(
                Arrays.asList("name", "type")
        );
        return new SqlProcessResult(enumerable, resultFields);
    }

    public Enumerable<Object[]> getEnumerable() {
        return enumerable;
    }

    public void setEnumerable(Enumerable<Object[]> enumerable) {
        this.enumerable = enumerable;
    }

    public List<RelDataTypeField> getFields() {
        return fields;
    }

    public void setFields(List<RelDataTypeField> fields) {
        this.fields = fields;
    }

    public boolean isCompleted() {
        return true;
    }
}
