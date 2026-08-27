package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.executor.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQuickStart {
    private static SqlExecutor sqlExecutor;

    @Test
    void testQuickStartFunction() throws Exception {
        String moduleDir = Paths.get(TestQuickStart.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).getParent().getParent().toString();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(
                Paths.get(moduleDir, "src", "main", "sql").toString()
        );

        sqlExecutor = new SqlExecutor();
        sqlExecutor.executeSql("insert into user_interest_category1 values (1000001, 'pc', 100)");
        sqlExecutor.executeSql("""
                insert into category1_hot_item values
                ('pc', 1000001, 100),
                ('pc', 1000002, 90),
                ('pc', 1000003, 80),
                ('pc', 1000004, 70),
                ('pc', 1000005, 60)
                """);

        DataCheckUtils.check(
                sqlExecutor.executeSql("select category1 from user_interest_category1 where user_id = 1000001"),
                List.<Object[]>of(new Object[]{"pc"})
        );

        sqlExecutor.executeSql("cache table quick_start_user as select cast(1000001 as bigint) as id");

        CacheTable result = sqlExecutor.executeSql("call test_rec(quick_start_user)");
        List<Object[]> rows = result.scan(null).toList();

        List<String> lines = DataTransformUtils.formatAsTable(result.scan(null), result.getDataFields());
        lines.forEach(System.out::println);

        assertEquals(2, rows.size());
        rows.forEach(row -> assertEquals(1000001L, row[0]));
    }
}
