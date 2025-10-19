package com.sqlrec.runtime;

import com.sqlrec.common.schema.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Set;

public interface BindableInterface {
    Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context);
    List<RelDataTypeField> getReturnDataFields();
    boolean isParallelizable();
    Set<String> getReadTables();
    Set<String> getWriteTables();
}
