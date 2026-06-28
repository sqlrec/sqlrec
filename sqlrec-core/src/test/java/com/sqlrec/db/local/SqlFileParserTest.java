package com.sqlrec.db.local;

import com.sqlrec.sql.parser.SqlCreateApi;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import com.sqlrec.sql.parser.SqlReturn;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlFileParserTest {

    private SqlFileParser parser;

    @BeforeEach
    void setUp() {
        parser = new SqlFileParser("dummy");
    }

    @Test
    void testParseCreateTable() throws Exception {
        parser.parseContent("CREATE TABLE mydb.t1 (id INT, name VARCHAR(100))");

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
        assertEquals(0, parser.getUdfFunctionNodes().size());
        assertEquals(0, parser.getSqlFunctionNodeGroups().size());
    }

    @Test
    void testParseCreateTableWithConnector() throws Exception {
        parser.parseContent("""
            CREATE TABLE IF NOT EXISTS user_table (
              user_id BIGINT,
              gender STRING,
              age INT,
              occupation INT,
              zip_code STRING,
              PRIMARY KEY (user_id) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'url' = 'redis://localhost:6379/0'
            )
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
    }

    @Test
    void testParseCreateTableWithKafkaConnector() throws Exception {
        parser.parseContent("""
            CREATE TABLE rec_log_kafka (
              user_id BIGINT,
              movie_id BIGINT,
              title STRING,
              rec_reason STRING,
              req_time BIGINT,
              req_id STRING
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'rec_log',
              'properties.bootstrap.servers' = 'localhost:9092',
              'format' = 'json'
            )
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
    }

    @Test
    void testParseCreateTableWithMilvusConnector() throws Exception {
        parser.parseContent("""
            CREATE TABLE item_embedding (
              id BIGINT,
              title STRING,
              genres ARRAY<STRING>,
              embedding ARRAY<DOUBLE>,
              PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
              'connector' = 'milvus',
              'url' = 'http://localhost:19530',
              'token' = 'root:Milvus',
              'database' = 'default',
              'collection' = 'item_embedding'
            )
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
    }

    @Test
    void testParseCreateTableWithRedisListConnector() throws Exception {
        parser.parseContent("""
            CREATE TABLE IF NOT EXISTS global_hot_item (
              invert_key STRING,
              movie_id BIGINT,
              score FLOAT,
              PRIMARY KEY (invert_key) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'data-structure' = 'list',
              'url' = 'redis://localhost:6379/0'
            )
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
    }

    @Test
    void testParseCreateTableWithoutDatabase() throws Exception {
        parser.parseContent("CREATE TABLE t1 (id INT)");

        assertEquals(1, parser.getTableNodes().size());
        assertTrue(parser.getTableNodes().get(0) instanceof SqlCreateTable);
    }

    @Test
    void testParseCreateFunction() throws Exception {
        parser.parseContent("CREATE FUNCTION mydb.my_udf AS 'com.example.MyUdf'");

        assertEquals(1, parser.getUdfFunctionNodes().size());
        assertTrue(parser.getUdfFunctionNodes().get(0) instanceof SqlCreateFunction);
        assertEquals(0, parser.getTableNodes().size());
    }

    @Test
    void testParseCreateApi() throws Exception {
        parser.parseContent("CREATE API main_rec WITH main_rec");

        assertEquals(1, parser.getApiNodes().size());
        assertTrue(parser.getApiNodes().get(0) instanceof SqlCreateApi);
    }

    @Test
    void testParseCreateModel() throws Exception {
        parser.parseContent("""
            CREATE MODEL IF NOT EXISTS rank_model (
              user_id BIGINT,
              movie_id BIGINT,
              genres ARRAY<STRING>,
              gender STRING,
              age INT,
              occupation INT,
              zip_code STRING
            ) WITH (
              'model' = 'tzrec.wide_and_deep',
              'label_columns' = 'rating'
            )
            """);

        assertEquals(1, parser.getModelNodes().size());
        assertTrue(parser.getModelNodes().get(0) instanceof SqlCreateModel);
    }

    @Test
    void testParseCreateService() throws Exception {
        parser.parseContent("""
            CREATE SERVICE rank_service ON MODEL rank_model CHECKPOINT='v1_export'
            WITH (
              'NAMESPACE' = 'sqlrec'
            )
            """);

        assertEquals(1, parser.getServiceNodes().size());
        assertTrue(parser.getServiceNodes().get(0) instanceof SqlCreateService);
    }

    @Test
    void testParseSqlFunction() throws Exception {
        parser.parseContent("""
            CREATE SQL FUNCTION my_func;
            CACHE TABLE t AS SELECT 1 AS a;
            RETURN t
            """);

        assertEquals(1, parser.getSqlFunctionNodeGroups().size());
        List<SqlNode> group = parser.getSqlFunctionNodeGroups().get(0);
        assertEquals(3, group.size());
        assertTrue(group.get(0) instanceof SqlCreateSqlFunction);
        assertTrue(group.get(2) instanceof SqlReturn);
        assertEquals(0, parser.getTableNodes().size());
    }

    @Test
    void testParseSqlFunctionWithDefineInputTable() throws Exception {
        parser.parseContent("""
            CREATE SQL FUNCTION recall_fun;
            DEFINE INPUT TABLE user_info(
              user_id BIGINT,
              gender STRING,
              age INT
            );
            CACHE TABLE user_embedding AS CALL call_service('recall_service_user', user_info);
            CACHE TABLE vector_recall AS
            SELECT item_embedding.id AS movie_id
            FROM user_embedding JOIN item_embedding ON 1=1
            LIMIT 300;
            RETURN vector_recall
            """);

        assertEquals(1, parser.getSqlFunctionNodeGroups().size());
        List<SqlNode> group = parser.getSqlFunctionNodeGroups().get(0);
        assertTrue(group.get(0) instanceof SqlCreateSqlFunction);
        assertTrue(group.get(group.size() - 1) instanceof SqlReturn);
    }

    @Test
    void testParseSqlFunctionWithEmptyReturn() throws Exception {
        parser.parseContent("""
            CREATE SQL FUNCTION save_rec_item;
            DEFINE INPUT TABLE final_recall_item(
              user_id BIGINT,
              movie_id BIGINT
            );
            INSERT INTO rec_log_kafka SELECT * FROM final_recall_item;
            RETURN
            """);

        assertEquals(1, parser.getSqlFunctionNodeGroups().size());
        List<SqlNode> group = parser.getSqlFunctionNodeGroups().get(0);
        assertTrue(group.get(0) instanceof SqlCreateSqlFunction);
        assertTrue(group.get(group.size() - 1) instanceof SqlReturn);
    }

    @Test
    void testParseSqlFunctionNotTerminated() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> parser.parseContent("""
                    CREATE SQL FUNCTION my_func;
                    CACHE TABLE t AS SELECT 1 AS a
                    """));
        assertTrue(ex.getMessage().contains("not terminated with RETURN"));
    }

    @Test
    void testParseMultipleTypes() throws Exception {
        parser.parseContent("""
            CREATE TABLE t1 (id INT);
            CREATE FUNCTION my_udf AS 'com.example.Udf';
            CREATE SQL FUNCTION my_func;
            CACHE TABLE t AS SELECT 1 AS a;
            RETURN t;
            CREATE API my_api WITH my_func;
            CREATE MODEL my_model WITH ('type'='test');
            CREATE SERVICE my_svc ON MODEL my_model
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertEquals(1, parser.getUdfFunctionNodes().size());
        assertEquals(1, parser.getSqlFunctionNodeGroups().size());
        assertEquals(1, parser.getApiNodes().size());
        assertEquals(1, parser.getModelNodes().size());
        assertEquals(1, parser.getServiceNodes().size());
    }

    @Test
    void testParseEmptyContent() throws Exception {
        parser.parseContent("");

        assertEquals(0, parser.getTableNodes().size());
        assertEquals(0, parser.getUdfFunctionNodes().size());
        assertEquals(0, parser.getSqlFunctionNodeGroups().size());
    }

    @Test
    void testParseMultipleSqlFunctions() throws Exception {
        parser.parseContent("""
            CREATE SQL FUNCTION func1;
            CACHE TABLE t1 AS SELECT 1 AS a;
            RETURN t1;
            CREATE SQL FUNCTION func2;
            CACHE TABLE t2 AS SELECT 2 AS b;
            RETURN t2
            """);

        assertEquals(2, parser.getSqlFunctionNodeGroups().size());
        assertEquals(3, parser.getSqlFunctionNodeGroups().get(0).size());
        assertEquals(3, parser.getSqlFunctionNodeGroups().get(1).size());
    }

    @Test
    void testParseContentWithSemicolonsInStrings() throws Exception {
        parser.parseContent("""
            CREATE TABLE t1 (id INT, name VARCHAR(100));
            CREATE FUNCTION my_udf AS 'com.example;MyUdf'
            """);

        assertEquals(1, parser.getTableNodes().size());
        assertEquals(1, parser.getUdfFunctionNodes().size());
    }

    @Test
    void testParseMultipleTablesWithConnector() throws Exception {
        parser.parseContent("""
            CREATE TABLE user_table (
              user_id BIGINT,
              gender STRING,
              PRIMARY KEY (user_id) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'url' = 'redis://localhost:6379/0'
            );
            CREATE TABLE item_table (
              movie_id BIGINT,
              title STRING,
              genres ARRAY<STRING>,
              PRIMARY KEY (movie_id) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'url' = 'redis://localhost:6379/0'
            )
            """);

        assertEquals(2, parser.getTableNodes().size());
    }

    @Test
    void testParseRealBenchmarkScenario() throws Exception {
        parser.parseContent("""
            CREATE TABLE IF NOT EXISTS user_table (
              user_id BIGINT,
              gender STRING,
              age INT,
              occupation INT,
              zip_code STRING,
              PRIMARY KEY (user_id) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'url' = 'redis://localhost:6379/0'
            );
            CREATE TABLE IF NOT EXISTS item_table (
              movie_id BIGINT,
              title STRING,
              genres ARRAY<STRING>,
              PRIMARY KEY (movie_id) NOT ENFORCED
            ) WITH (
              'connector' = 'redis',
              'url' = 'redis://localhost:6379/0'
            );
            CREATE SQL FUNCTION recall_fun;
            DEFINE INPUT TABLE user_info(
              user_id BIGINT,
              gender STRING,
              age INT,
              occupation INT,
              zip_code STRING
            );
            CACHE TABLE user_embedding AS CALL call_service('recall_service_user', user_info);
            RETURN user_embedding;
            CREATE SQL FUNCTION rank_fun;
            DEFINE INPUT TABLE user_info(
              user_id BIGINT,
              gender STRING,
              age INT
            );
            CACHE TABLE rank_feature AS
            SELECT user_info.user_id, user_info.age
            FROM user_info;
            RETURN rank_feature;
            CREATE API main_rec WITH main_rec;
            CREATE MODEL IF NOT EXISTS rank_model (
              user_id BIGINT,
              movie_id BIGINT,
              genres ARRAY<STRING>,
              gender STRING,
              age INT
            ) WITH (
              'model' = 'tzrec.wide_and_deep',
              'label_columns' = 'rating'
            );
            CREATE SERVICE rank_service ON MODEL rank_model CHECKPOINT='v1_export'
            WITH (
              'NAMESPACE' = 'sqlrec'
            )
            """);

        assertEquals(2, parser.getTableNodes().size());
        assertEquals(2, parser.getSqlFunctionNodeGroups().size());
        assertEquals(1, parser.getApiNodes().size());
        assertEquals(1, parser.getModelNodes().size());
        assertEquals(1, parser.getServiceNodes().size());
    }

    @Test
    void testResolveVariablesNoPlaceholders() {
        Map<String, String> vars = Map.of();
        String content = "CREATE TABLE t1 (id INT)";
        assertEquals(content, parser.resolveVariables(content, vars));
    }

    @Test
    void testResolveVariablesSingleReplacement() {
        Map<String, String> vars = Map.of("REDIS_HOST", "redis://prod:6379");
        String result = parser.resolveVariables("'url' = '${REDIS_HOST}'", vars);
        assertEquals("'url' = 'redis://prod:6379'", result);
    }

    @Test
    void testResolveVariablesMultipleReplacements() {
        Map<String, String> vars = Map.of("KAFKA_HOST", "kafka-broker", "KAFKA_PORT", "9092");
        String result = parser.resolveVariables("'servers' = '${KAFKA_HOST}:${KAFKA_PORT}'", vars);
        assertEquals("'servers' = 'kafka-broker:9092'", result);
    }

    @Test
    void testResolveVariablesNotFoundKeepsPlaceholder() {
        Map<String, String> vars = Map.of();
        String content = "'url' = '${SQLREC_NONEXISTENT_VAR_12345}'";
        String result = parser.resolveVariables(content, vars);
        assertEquals(content, result);
    }

    @Test
    void testResolveVariablesEmptyVarName() {
        Map<String, String> vars = Map.of();
        String content = "'url' = '${}'";
        String result = parser.resolveVariables(content, vars);
        assertEquals(content, result);
    }

    @Test
    void testResolveVariablesUnclosedBraceKeepsOriginal() {
        Map<String, String> vars = Map.of();
        String content = "'url' = '${UNCLOSED'";
        String result = parser.resolveVariables(content, vars);
        assertEquals(content, result);
    }

    @Test
    void testResolveVariablesDollarSignWithoutBrace() {
        Map<String, String> vars = Map.of();
        String content = "price = $100";
        assertEquals(content, parser.resolveVariables(content, vars));
    }

    @Test
    void testResolveVariablesEmptyContent() {
        Map<String, String> vars = Map.of();
        assertEquals("", parser.resolveVariables("", vars));
    }

    @Test
    void testResolveVariablesAdjacentPlaceholders() {
        Map<String, String> vars = Map.of("A", "hello", "B", "world");
        String result = parser.resolveVariables("${A}${B}", vars);
        assertEquals("helloworld", result);
    }
}
