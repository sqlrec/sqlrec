package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.executor.SqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;

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
        SqlExecutor sqlExecutor = new SqlExecutor();
        
        DataCheckUtils.check(
                sqlExecutor.executeSql("cache table t1 as select 1 as id, 'hello' as name"), 
                Arrays.<Object[]>asList(new Object[]{"t1", 1L})
        );

        DataCheckUtils.check(
                sqlExecutor.executeSql("cache table t2 as call demo_table_udf(t1)"), 
                Arrays.<Object[]>asList(new Object[]{"t2", 1L})
        );

        DataCheckUtils.check(
                sqlExecutor.executeSql("select * from t2"), 
                Arrays.<Object[]>asList(new Object[]{1, "hello"})
        );
    }

    @Test
    void testDemoScalarUdf() throws Exception {
        SqlExecutor sqlExecutor = new SqlExecutor();

        DataCheckUtils.check(
                sqlExecutor.executeSql("select demo_scalar_udf('hello')"),
                Arrays.<Object[]>asList(new Object[]{"hello"})
        );

        DataCheckUtils.check(
                sqlExecutor.executeSql("select demo_scalar_udf('world')"),
                Arrays.<Object[]>asList(new Object[]{"world"})
        );

        DataCheckUtils.check(
                sqlExecutor.executeSql("select demo_scalar_udf('')"),
                Arrays.<Object[]>asList(new Object[]{""})
        );
    }
}
