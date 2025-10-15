package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class SetBindable implements BindableInterface {
    private final String key;
    private final String value;

    public SetBindable(String k, String v) {
        this.key = k;
        this.value = v;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        context.setVariable(key, value);
        return null;
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return null;
    }
}
