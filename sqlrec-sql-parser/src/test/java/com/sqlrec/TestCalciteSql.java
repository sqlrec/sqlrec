package com.sqlrec;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestCalciteSql {
    @Test
    public void testCalciteSql() throws SqlParseException {
        // Configure SQL parser
        SqlParser.Config parserConfig = SqlParser.config()
                .withConformance(SqlConformanceEnum.DEFAULT)
                .withParserFactory(FlinkSqlParserImpl.FACTORY);

        List<String> sqlList = Arrays.asList(
                "show sql functions",
                "show apis",
                "desc sql function fun1",
                "desc api api1",
                "create api api1 with fun1",
                "cache table t2 as select * from t1 where id=1",
                "cache table t2 as fun1(t1)",
                "call fun1(t1)",
                "create sql function fun1",
                "define input table t1(id int, name string)",
                "return t1",
                "return"
        );

        for (String sql : sqlList) {
            SqlParser parser = SqlParser.create(sql, parserConfig);
            SqlNode sqlNode = parser.parseQuery();
            System.out.println(sqlNode.getClass());
        }
    }
}