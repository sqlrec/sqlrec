package com.sqlrec.flink;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.JsonRows;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class FlinkParserCompatibilityTest {
    @Test
    void standaloneParserAndIsolatedPlannerCanBothReadFunctionDeclarations() throws Exception {
        String sql = "CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF'";
        SqlCreateFunction statement = (SqlCreateFunction) SqlParser.create(sql,
                SqlParser.config().withParserFactory(FlinkSqlParserImpl.FACTORY)).parseStmt();
        assertArrayEquals(new String[]{"BATCH_CALL_SERVICE"}, statement.getFunctionIdentifier());
        StreamTableEnvironment environment = StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment());
        environment.executeSql(sql);
    }

    @Test
    void sharedSchemaConversionWorksWithTheBundledParser() {
        assertArrayEquals(new Object[]{Arrays.asList(0.25d, null), List.of("first", "2")},
                JsonRows.decode("{\"scores\":[0.25,null],\"names\":[\"first\",2]}",
                        List.of(new FieldSchema("scores", "ARRAY<FLOAT>"),
                                new FieldSchema("names", "ARRAY<STRING>"))));
    }
}
