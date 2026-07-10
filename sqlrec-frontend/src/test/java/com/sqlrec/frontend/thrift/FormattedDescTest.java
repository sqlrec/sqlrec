package com.sqlrec.frontend.thrift;

import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.executor.SqlProcessResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@Tag("integration")
public class FormattedDescTest {
    @Test
    public void testFormattedDesc() throws Exception {
        List<String> sqlList = Arrays.asList(
                "describe model rank_model",
                "describe formatted model rank_model",
                "describe model rank_model checkpoint='v1'",
                "describe formatted model rank_model checkpoint='v1'",
                "describe service rank_service",
                "describe formatted service rank_service"
        );

        SqlExecutor executor = new SqlExecutor();
        for (String sql : sqlList) {
            System.out.println("\n\n" + sql);
            SqlProcessResult result = executor.executeSqlAsync(sql);
            List<Object[]> actualResults = result.getEnumerable().toList();
            for (Object[] row : actualResults) {
                System.out.println(java.util.Arrays.toString(row));
            }
        }
    }
}
