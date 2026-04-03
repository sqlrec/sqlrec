package com.sqlrec;

import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.SchemaUtils;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUdfSupport {
    @Test
    public void testUdfSupport() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(Const.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("myTable", new TestTypeSupport.MyTable());
            }
        });
        HmsSchema.setGlobalSchema(schema);

        SchemaUtils.addFunction(
                schema.getSubSchema(Const.DEFAULT_SCHEMA_NAME, false),
                "uuid",
                "com.sqlrec.udf.scalar.UuidFunction"
        );
        SchemaUtils.addFunction(
                schema.getSubSchema(Const.DEFAULT_SCHEMA_NAME, false),
                "l2_norm",
                "com.sqlrec.udf.scalar.L2NormFunction"
        );

        List<String> sqlList = Arrays.asList(
                "select uuid()",
                "select l2_norm(array_float_type) from myTable",
                "select l2_norm(array_double_type) from myTable",
                "select SIN(0.1)",
                "select count(1) from myTable",
                "select sum(int_type) from myTable",
                "select min(int_type) from myTable",
                "select max(int_type) from myTable",
                "select UPPER(varchar_type) from myTable",
                "select CHAR_LENGTH(varchar_type) from myTable",
                "select SUBSTRING(varchar_type from 1 for 2) from myTable",
                "select varchar_type || '1' from myTable",
                "select CARDINALITY(array_int_type) from myTable",
                "select CARDINALITY(array_varchar_type) from myTable",
                "select CARDINALITY(array_float_type) from myTable",
                "select CARDINALITY(array_double_type) from myTable",
                "select CURRENT_TIMESTAMP",
                "select CURRENT_TIMESTAMP(1)",
                "select cast(CURRENT_TIMESTAMP as BIGINT) as req_time"
        );

        for (String sql : sqlList) {
            new SqlTestCase(sql).test(schema, new ExecuteContextImpl());
        }
    }
}
