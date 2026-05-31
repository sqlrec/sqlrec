package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TestDemoUdf {

    @BeforeAll
    static void setUp() throws URISyntaxException {
        String moduleDir = Paths.get(TestDemoUdf.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).getParent().getParent().toString();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(
                Paths.get(moduleDir, "src", "main", "sql").toString()
        );
    }

    @Test
    void testDemoTableUdf() throws Exception {
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        ExecuteContextImpl executeContext = new ExecuteContextImpl();

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as select 1 as id, 'hello' as name",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "cache table t2 as call demo_table_udf(t1)",
                        Arrays.<Object[]>asList(new Object[]{"t2", 1L})
                ),
                new SqlTestCase(
                        "select * from t2",
                        Arrays.<Object[]>asList(new Object[]{1, "hello"})
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }

    @Test
    void testDemoScalarUdf() throws Exception {
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        ExecuteContextImpl executeContext = new ExecuteContextImpl();

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "select demo_scalar_udf('hello')",
                        Arrays.<Object[]>asList(
                                new Object[]{"hello"}
                        )
                ),
                new SqlTestCase(
                        "select demo_scalar_udf('world')",
                        Arrays.<Object[]>asList(
                                new Object[]{"world"}
                        )
                ),
                new SqlTestCase(
                        "select demo_scalar_udf('')",
                        Arrays.<Object[]>asList(
                                new Object[]{""}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }
}
