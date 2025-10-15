package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public interface BindableInterface {
    Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context);
    List<RelDataTypeField> getReturnDataFields();
}
