package com.sqlrec;

import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestUnion {
    @Test
    public void testUnionSql() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        List<SqlTestCase> sqlList = Arrays.asList(
                new SqlTestCase(
                        "cache table t0 as select 1 as a union all select 2 as a union all select 3 as a",
                        Arrays.<Object[]>asList(
                                new Object[]{"t0", 3L}
                        )
                ),
                new SqlTestCase(
                        "cache table t1 as select 4 as a union all select 5 as a union all select 6 as a",
                        Arrays.<Object[]>asList(
                                new Object[]{"t1", 3L}
                        )
                ),
                new SqlTestCase(
                        "cache table t2 as select 7 as a union all select 8 as a union all select 9 as a",
                        Arrays.<Object[]>asList(
                                new Object[]{"t2", 3L}
                        )
                ),
                new SqlTestCase(
                        "select * from t0 union all select * from t1 union all select * from t2",
                        Arrays.asList(
                                new Object[]{1},
                                new Object[]{4},
                                new Object[]{7},
                                new Object[]{2},
                                new Object[]{5},
                                new Object[]{8},
                                new Object[]{3},
                                new Object[]{6},
                                new Object[]{9}
                        )
                )
        );

        for (SqlTestCase sqlTestCase : sqlList) {
            sqlTestCase.test(schema);
        }
    }
}
