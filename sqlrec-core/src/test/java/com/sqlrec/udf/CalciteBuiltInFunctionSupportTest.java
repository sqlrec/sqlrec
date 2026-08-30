package com.sqlrec.udf;

import com.sqlrec.common.config.Consts;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Verifies the Calcite built-in functions exposed by
 * {@code SqlStdOperatorTable} through SQLRec's complete compile and execute path.
 */
public class CalciteBuiltInFunctionSupportTest {
    private CalciteSchema schema;
    private ExecuteContextImpl executeContext;

    @BeforeEach
    public void setUp() {
        schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        CalciteSchemaFactory.setGlobalSchema(schema);
        executeContext = new ExecuteContextImpl();
    }

    @Test
    public void testNumericFunctions() throws Exception {
        assertSql(
                "select ABS(-2), MOD(5, 2), "
                        + "POWER(cast(2 as double), 3), SQRT(cast(9 as double)), "
                        + "SIGN(-8), CEIL(cast(1.2 as double)), FLOOR(cast(1.8 as double)), "
                        + "ROUND(cast(1.25 as double), 1), TRUNCATE(cast(1.29 as double), 1)",
                row(2, 1, 8.0d, 3.0d, -1, 2.0d, 1.0d, 1.3d, 1.2d)
        );

        assertSql(
                "select ACOS(cast(1 as double)), ASIN(cast(0 as double)), "
                        + "ATAN(cast(1 as double)), ATAN2(cast(1 as double), cast(2 as double)), "
                        + "CBRT(cast(8 as double)), COS(cast(0 as double)), "
                        + "COT(cast(1 as double)), DEGREES(cast(1 as double)), "
                        + "EXP(cast(1 as double)), LN(cast(2 as double)), LOG10(cast(100 as double)), "
                        + "RADIANS(cast(180 as double)), SIN(cast(0 as double)), TAN(cast(0 as double))",
                row(
                        Math.acos(1.0d),
                        Math.asin(0.0d),
                        Math.atan(1.0d),
                        Math.atan2(1.0d, 2.0d),
                        Math.cbrt(8.0d),
                        Math.cos(0.0d),
                        1.0d / Math.tan(1.0d),
                        Math.toDegrees(1.0d),
                        Math.exp(1.0d),
                        Math.log(2.0d),
                        Math.log10(100.0d),
                        Math.toRadians(180.0d),
                        Math.sin(0.0d),
                        Math.tan(0.0d)
                )
        );

        // PI is a Calcite niladic function and therefore has no parentheses.
        assertSql("select PI", row(Math.PI));
    }

    @Test
    public void testStringFunctions() throws Exception {
        assertSql(
                "select ASCII('A'), CHAR_LENGTH('abc'), CHARACTER_LENGTH('abc'), "
                        + "UPPER('SqlRec'), LOWER('SqlRec'), INITCAP('hello WORLD'), "
                        + "POSITION('b' IN 'abc'), "
                        + "OVERLAY('abcdef' PLACING 'ZZ' FROM 2 FOR 2), "
                        + "REPLACE('abcabc', 'b', 'X'), "
                        + "SUBSTRING('abcdef' FROM 2 FOR 3), TRIM('  abc  '), "
                        + "'sql' || 'rec'",
                row(65, 3, 3, "SQLREC", "sqlrec", "Hello World", 2,
                        "aZZdef", "aXcaXc", "bcd", "abc", "sqlrec")
        );
    }

    @Test
    public void testNullConditionalAndCastFunctions() throws Exception {
        assertSql(
                "select COALESCE(cast(null as integer), 2), "
                        + "NULLIF(1, 1), NULLIF(1, 2), "
                        + "CAST('12' AS BIGINT), "
                        + "CASE WHEN 2 > 1 THEN 'yes' ELSE 'no' END",
                row(2, null, 1, 12L, "yes")
        );
    }

    @Test
    public void testDateTimeFunctions() throws Exception {
        assertSql(
                "select EXTRACT(YEAR FROM DATE '2024-03-04'), "
                        + "YEAR(DATE '2024-03-04'), QUARTER(DATE '2024-03-04'), "
                        + "MONTH(DATE '2024-03-04'), WEEK(DATE '2024-03-04'), "
                        + "DAYOFYEAR(DATE '2024-03-04'), DAYOFMONTH(DATE '2024-03-04'), "
                        + "DAYOFWEEK(DATE '2024-03-04'), "
                        + "HOUR(TIMESTAMP '2024-03-04 12:34:56'), "
                        + "MINUTE(TIMESTAMP '2024-03-04 12:34:56'), "
                        + "SECOND(TIMESTAMP '2024-03-04 12:34:56')",
                row(2024L, 2024L, 1L, 3L, 10L, 64L, 4L, 2L, 12L, 34L, 56L)
        );

        assertSql(
                "select "
                        + "TIMESTAMPADD(DAY, 2, TIMESTAMP '2024-01-01 00:00:00') "
                        + "= TIMESTAMP '2024-01-03 00:00:00', "
                        + "TIMESTAMPDIFF(DAY, TIMESTAMP '2024-01-01 00:00:00', "
                        + "TIMESTAMP '2024-01-03 00:00:00'), "
                        + "LAST_DAY(DATE '2024-02-03') = DATE '2024-02-29'",
                row(true, 2, true)
        );
    }

    @Test
    public void testCollectionFunctions() throws Exception {
        assertSql(
                "select CARDINALITY(ARRAY[1, 2, 3]), "
                        + "ARRAY[1, 2, 3][1], MAP['a', 1, 'b', 2]['b'], "
                        + "ELEMENT(MULTISET[7])",
                row(3, 1, 2, 7)
        );
    }

    @Test
    public void testBasicAggregateFunctions() throws Exception {
        assertSql(
                "select COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x), "
                        + "EVERY(x > 0), SOME(x > 2), APPROX_COUNT_DISTINCT(x) "
                        + "from (values (1), (2), (3)) as t(x)",
                row(3L, 6, 2, 1, 3, true, true, 3L)
        );

        assertSql(
                "select ANY_VALUE(x), SINGLE_VALUE(x) from (values (7)) as t(x)",
                row(7, 7)
        );

        assertSql(
                "select MODE(x) from (values (1), (1), (2)) as t(x)",
                row(1)
        );
    }

    @Test
    public void testStatisticalAndBitAggregateFunctions() throws Exception {
        assertSql(
                "select STDDEV_POP(x), STDDEV_SAMP(x), STDDEV(x), "
                        + "VAR_POP(x), VAR_SAMP(x), VARIANCE(x), "
                        + "COVAR_POP(x, x), COVAR_SAMP(x, x), "
                        + "REGR_COUNT(x, x), REGR_SXX(x, x), REGR_SYY(x, x) "
                        + "from (values (cast(1 as double)), (cast(1 as double)), "
                        + "(cast(1 as double))) as t(x)",
                row(0.0d, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d,
                        0.0d, 0.0d, 3L, 0.0d, 0.0d)
        );

        assertSql(
                "select BIT_AND(x), BIT_OR(x), BIT_XOR(x) "
                        + "from (values (1), (2), (3)) as t(x)",
                row(0, 3, 0)
        );
    }

    @Test
    public void testCollectionAndStringAggregateFunctions() throws Exception {
        assertSql(
                "select COLLECT(x), LISTAGG(c, ',') "
                        + "from (values (1, 'a'), (2, 'b')) as t(x, c)",
                row(Arrays.asList(1, 2), "a,b")
        );

        assertSql(
                "select FUSION(ms), INTERSECTION(ms) "
                        + "from (values (MULTISET[1, 2]), (MULTISET[2, 3])) as t(ms)",
                row(Arrays.asList(1, 2, 2, 3), Arrays.asList(2))
        );
    }

    @Test
    public void testGroupingFunctions() throws Exception {
        assertSql(
                "select x, GROUPING(x), GROUPING_ID(x), GROUP_ID(), COUNT(*) "
                        + "from (values (1), (2)) as t(x) "
                        + "group by ROLLUP(x) order by x nulls last",
                row(1, 0L, 0L, 0L, 1L),
                row(2, 0L, 0L, 0L, 1L),
                row(null, 1L, 1L, 0L, 2L)
        );
    }

    @Test
    public void testWindowFunctions() throws Exception {
        assertSql(
                "select x, "
                        + "ROW_NUMBER() over (order by x), "
                        + "RANK() over (order by x), "
                        + "DENSE_RANK() over (order by x), "
                        + "NTILE(2) over (order by x), "
                        + "FIRST_VALUE(x) over (order by x), "
                        + "LAST_VALUE(x) over (order by x rows between unbounded preceding and unbounded following), "
                        + "NTH_VALUE(x, 2) over (order by x rows between unbounded preceding and unbounded following), "
                        + "LEAD(x) over (order by x), LAG(x) over (order by x) "
                        + "from (values (1), (2), (3)) as t(x) order by x",
                row(1, 1L, 1L, 1L, 1L, 1, 3, 2, 2, null),
                row(2, 2L, 2L, 2L, 1L, 1, 3, 2, 3, 1),
                row(3, 3L, 3L, 3L, 2L, 1, 3, 2, null, 2)
        );
    }

    @Test
    public void testJsonFunctions() throws Exception {
        assertSql(
                "select JSON_EXISTS('{\"a\":1}', 'strict $.a'), "
                        + "JSON_VALUE('{\"a\":1}', 'strict $.a'), "
                        + "JSON_QUERY('{\"a\":[1,2]}', 'strict $.a'), "
                        + "JSON_TYPE('{\"a\":1}'), JSON_DEPTH('{\"a\":1}'), "
                        + "JSON_LENGTH('[1,2,3]'), JSON_REMOVE('{\"a\":1}', '$.a')",
                row(true, "1", "[1,2]", "OBJECT", 2, 3, "{}")
        );

        assertSql(
                "select JSON_OBJECT(KEY 'a' VALUE 1), JSON_ARRAY(1, 2), "
                        + "JSON_ARRAYAGG(x), JSON_OBJECTAGG(KEY c VALUE x) "
                        + "from (values (1, 'a')) as t(x, c)",
                row("{\"a\":1}", "[1,2]", "[1]", "{\"a\":1}")
        );
    }

    private void assertSql(String sql, Object[]... expectedRows) throws Exception {
        List<Object[]> expectedResult = Arrays.asList(expectedRows);
        new SqlTestCase(sql, expectedResult)
                .setDebugOutput(false)
                .test(schema, executeContext);
    }

    private static Object[] row(Object... values) {
        return values;
    }
}
