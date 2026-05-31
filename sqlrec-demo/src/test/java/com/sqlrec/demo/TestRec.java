package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@Tag("integration")
public class TestRec {
    @BeforeAll
    static void setUp() throws URISyntaxException {
        String moduleDir = Paths.get(TestDemoUdf.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).getParent().getParent().toString();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(
                Paths.get(moduleDir, "src", "main", "sql").toString()
        );
    }

    @Test
    void testRec() throws Exception {
        CalciteSchema schema = CalciteSchemaFactory.createCalciteSchema();
        ExecuteContextImpl executeContext = new ExecuteContextImpl();

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t1 as select cast(1 as bigint) as user_id",
                        Arrays.<Object[]>asList(new Object[]{"t1", 1L})
                ),
                new SqlTestCase(
                        "call main_rec(t1)"
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema, executeContext);
        }
    }
}
