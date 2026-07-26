package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.executor.SqlExecutor;
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
        SqlExecutor sqlExecutor = new SqlExecutor();

        sqlExecutor.executeSql("set 'rank_fun'='rank_fun_simple'");

        DataCheckUtils.check(
                sqlExecutor.executeSql("cache table t1 as select cast(1 as bigint) as user_id"),
                Arrays.<Object[]>asList(new Object[]{"t1", 1L})
        );

        CacheTable result = sqlExecutor.executeSql("call main_rec(t1)");
        List<String> lines = DataTransformUtils.formatAsTable(result.scan(null), result.getDataFields());
        lines.forEach(System.out::println);
    }
}
