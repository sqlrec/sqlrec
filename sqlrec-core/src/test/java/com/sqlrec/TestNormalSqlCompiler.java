package com.sqlrec;

import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestNormalSqlCompiler {

    public static void main(String[] args) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestCalciteSql.MyTable());
            }
        });

        String sql = "SELECT NAME, count(*) as cnt FROM myTable where ID > 1 group by NAME";

        BindableInterface bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

        Enumerable enumerable = bindable.bind(schema);
        List<Object[]> results = enumerable.toList();
        for (Object[] result : results) {
            System.out.println(java.util.Arrays.toString(result));
        }
    }
}
