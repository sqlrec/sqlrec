package com.sqlrec.frontend.service;

import com.sqlrec.frontend.common.SqlProcessResult;
import com.sqlrec.frontend.common.SqlProcessor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@Tag("integration")
public class FormattedDescTest {
    @Test
    public void testFormattedDesc() {
        List<String> sqlList = Arrays.asList(
                "describe model test_model",
                "describe formatted model test_model",
                "describe model test_model checkpoint='test22'",
                "describe formatted model test_model checkpoint='test22'",
                "describe service test_service",
                "describe formatted service test_service"
        );

        SqlProcessor processor = new SqlProcessor();
        for (String sql : sqlList) {
            System.out.println("\n\n" + sql);
            SqlProcessResult rowSet = processor.tryExecuteSql(sql);
            List<Object[]> actualResults = rowSet.getEnumerable().toList();
            for (Object[] result : actualResults) {
                System.out.println(java.util.Arrays.toString(result));
            }
        }
    }
}
