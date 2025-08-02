package com.sqlrec.runtime;

import com.sqlrec.schema.CacheTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CacheTableBindable implements BindableInterface {
    private String tableName;
    private BindableInterface bindable;

    public CacheTableBindable(String tableName, BindableInterface bindable) {
        this.tableName = tableName;
        this.bindable = bindable;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema) {
        Enumerable<Object[]> enumerable = bindable.bind(schema);
        schema.add(tableName, new CacheTable(tableName, enumerable, bindable.getReturnDataFields()));

        // return cache table counts
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{tableName, (long) enumerable.count()});
        return Linq4j.asEnumerable(list);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return Arrays.asList(
                new RelDataTypeFieldImpl(
                        "table_name",
                        0,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)
                ),
                new RelDataTypeFieldImpl(
                        "count",
                        1,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT)
                )
        );
    }

    public String getTableName() {
        return tableName;
    }
}
