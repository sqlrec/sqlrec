package com.sqlrec.compiler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompileManagerGetSqlStrTest {

    private static String formatSql(String sql) throws Exception {
        return CompileManager.getSqlStr(CompileManager.parseFlinkSql(sql))
                .replace("\r\n", "\n");
    }

    @Test
    void testSimpleSelect() throws Exception {
        String result = formatSql("select a, b from t");
        assertEquals("""
SELECT `a`,
    `b`
FROM `t`""", result);
    }

    @Test
    void testSelectWithWhere() throws Exception {
        String result = formatSql("select a, b from t where c = 1");
        assertEquals("""
SELECT `a`,
    `b`
FROM `t`
WHERE `c` = 1""", result);
    }

    @Test
    void testSelectWithGroupByHaving() throws Exception {
        String result = formatSql("select a, count(b) as cnt from t group by a having count(b) > 1");
        assertEquals("""
SELECT `a`,
    COUNT(`b`) AS `cnt`
FROM `t`
GROUP BY `a`
HAVING COUNT(`b`) > 1""", result);
    }

    @Test
    void testSelectWithOrderByLimit() throws Exception {
        String result = formatSql("select a from t order by a desc limit 10");
        assertEquals("""
SELECT `a`
FROM `t`
ORDER BY `a` DESC
FETCH NEXT 10 ROWS ONLY""", result);
    }

    @Test
    void testSelectWithJoin() throws Exception {
        String result = formatSql("select t1.a, t2.b from t1 join t2 on t1.id = t2.id");
        assertEquals("""
SELECT `t1`.`a`,
    `t2`.`b`
FROM `t1`
    INNER JOIN `t2` ON `t1`.`id` = `t2`.`id`""", result);
    }

    @Test
    void testSelectWithLeftJoin() throws Exception {
        String result = formatSql("select u.user_id, e.movie_id from user_info u left join user_exposure_item e on u.user_id = e.user_id");
        assertEquals("""
SELECT `u`.`user_id`,
    `e`.`movie_id`
FROM `user_info` AS `u`
    LEFT JOIN `user_exposure_item` AS `e` ON `u`.`user_id` = `e`.`user_id`""", result);
    }

    @Test
    void testSelectWithSubQuery() throws Exception {
        String result = formatSql("select a from (select a, b from t) sub");
        assertEquals("""
SELECT `a`
FROM (SELECT `a`,
            `b`
        FROM `t`) AS `sub`""", result);
    }

    @Test
    void testUnionAll() throws Exception {
        String result = formatSql("select a, b from t1 union all select a, b from t2");
        assertEquals("""
SELECT `a`,
    `b`
FROM `t1`
UNION ALL
SELECT `a`,
    `b`
FROM `t2`""", result);
    }

    @Test
    void testInsertInto() throws Exception {
        String result = formatSql("insert into t select a, b from s");
        assertEquals("""
INSERT INTO `t`
(SELECT `a`,
        `b`
    FROM `s`)""", result);
    }

    @Test
    void testCreateTable() throws Exception {
        String result = formatSql("CREATE TABLE IF NOT EXISTS user_table (user_id BIGINT, gender STRING, age INT, PRIMARY KEY (user_id) NOT ENFORCED) WITH ('connector' = 'redis', 'url' = 'redis://localhost:6379/0')");
        assertEquals("""
CREATE TABLE IF NOT EXISTS `user_table` (
  `user_id` BIGINT,
  `gender` STRING,
  `age` INTEGER,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://localhost:6379/0'
)""", result);
    }

    @Test
    void testSetStatement() throws Exception {
        String result = formatSql("SET 'table.sql-dialect'='default'");
        assertEquals("SET 'table.sql-dialect' = 'default'", result);
    }

    @Test
    void testSelectWithListagg() throws Exception {
        String result = formatSql("select movie_id, LISTAGG(distinct rec_reason) as rec_reason from t group by movie_id");
        assertEquals("""
SELECT `movie_id`,
    LISTAGG(DISTINCT `rec_reason`) AS `rec_reason`
FROM `t`
GROUP BY `movie_id`""", result);
    }

    @Test
    void testSelectWithCastAndFunction() throws Exception {
        String result = formatSql("select user_id, cast(CURRENT_TIMESTAMP as BIGINT) as req_time, uuid() as req_id from user_info");
        assertEquals("""
SELECT `user_id`,
    CAST(CURRENT_TIMESTAMP AS BIGINT) AS `req_time`,
    `uuid`() AS `req_id`
FROM `user_info`""", result);
    }

    @Test
    void testComplexMultiJoin() throws Exception {
        String result = formatSql("select recall_item.movie_id, recall_item.rec_reason, item_table.genres, item_table.title from recall_item join item_table on recall_item.movie_id = item_table.movie_id");
        assertEquals("""
SELECT `recall_item`.`movie_id`,
    `recall_item`.`rec_reason`,
    `item_table`.`genres`,
    `item_table`.`title`
FROM `recall_item`
    INNER JOIN `item_table` ON `recall_item`.`movie_id` = `item_table`.`movie_id`""", result);
    }

    @Test
    void testSelectWithMultipleJoinsAndWhere() throws Exception {
        String result = formatSql("select movie_id from user_info join user_exposure_item on user_exposure_item.user_id = user_info.user_id where bhv_time > cast(CURRENT_TIMESTAMP as BIGINT) - 3600000 group by movie_id");
        assertEquals("""
SELECT `movie_id`
FROM `user_info`
    INNER JOIN `user_exposure_item` ON `user_exposure_item`.`user_id` = `user_info`.`user_id`
WHERE `bhv_time` > CAST(CURRENT_TIMESTAMP AS BIGINT) - 3600000
GROUP BY `movie_id`""", result);
    }

    @Test
    void testSelectWithAggregateAndOrderBy() throws Exception {
        String result = formatSql("select movie_id from t group by movie_id order by MAX(bhv_time) desc limit 10");
        assertEquals("""
SELECT `movie_id`
FROM `t`
GROUP BY `movie_id`
ORDER BY MAX(`bhv_time`) DESC
FETCH NEXT 10 ROWS ONLY""", result);
    }

    @Test
    void testCreateModel() throws Exception {
        String result = formatSql("create model if not exists rank_model (user_id BIGINT, movie_id BIGINT, genres ARRAY<STRING>) with ('model'='tzrec.wide_and_deep', 'label_columns'='rating')");
        assertEquals("""
CREATE MODEL IF NOT EXISTS `rank_model` (
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `genres` ARRAY< STRING >
) WITH (
  'model' = 'tzrec.wide_and_deep',
  'label_columns' = 'rating'
)""", result);
    }

    @Test
    void testTrainModel() throws Exception {
        String result = formatSql("train model rank_model checkpoint='v1' on ml_sample with ('NAMESPACE'='sqlrec', 'batch_size'='1024')");
        assertEquals("""
TRAIN MODEL `rank_model` checkpoint= 'v1' ON `ml_sample` WITH (
  'NAMESPACE' = 'sqlrec',
  'batch_size' = '1024'
)""", result);
    }

    @Test
    void testExportModel() throws Exception {
        String result = formatSql("export model rank_model checkpoint='v1' on ml_sample with ('NAMESPACE'='sqlrec')");
        assertEquals("""
EXPORT MODEL `rank_model` checkpoint= 'v1' ON `ml_sample` WITH (
  'NAMESPACE' = 'sqlrec'
)""", result);
    }

    @Test
    void testCreateService() throws Exception {
        String result = formatSql("create service rank_service on model rank_model checkpoint='v1_export' with ('NAMESPACE'='sqlrec')");
        assertEquals("""
CREATE SERVICE `rank_service` ON MODEL `rank_model` checkpoint= 'v1_export' WITH (
  'NAMESPACE' = 'sqlrec'
)""", result);
    }

    @Test
    void testCacheTableAsSelect() throws Exception {
        String result = formatSql("cache table t1 as select 1 as user_id, 100 as movie_id");
        assertEquals("""
CACHE TABLE `t1` AS SELECT 1 AS `user_id`,
    100 AS `movie_id`""", result);
    }

    @Test
    void testCacheTableAsCall() throws Exception {
        String result = formatSql("cache table user_embedding as call call_service('recall_service_user', user_info)");
        assertEquals("CACHE TABLE `user_embedding` AS CALL `call_service` ('recall_service_user', `user_info`)", result);
    }

    @Test
    void testCreateSqlFunction() throws Exception {
        String result = formatSql("create or replace sql function recall_fun");
        assertEquals("CREATE OR REPLACE SQL FUNCTION `recall_fun`", result);
    }

    @Test
    void testDefineInputTable() throws Exception {
        String result = formatSql("define input table user_info(user_id bigint, gender string, age int)");
        assertEquals("""
DEFINE INPUT TABLE `user_info` (
  `user_id` BIGINT,
  `gender` STRING,
  `age` INTEGER
)""", result);
    }

    @Test
    void testReturnTable() throws Exception {
        String result = formatSql("return dedup_recall_item");
        assertEquals("RETURN `dedup_recall_item`", result);
    }

    @Test
    void testReturnEmpty() throws Exception {
        String result = formatSql("return");
        assertEquals("RETURN", result);
    }

    @Test
    void testCreateApi() throws Exception {
        String result = formatSql("create or replace api main_rec with main_rec");
        assertEquals("CREATE OR REPLACE API `main_rec` WITH `main_rec`", result);
    }

    @Test
    void testDropModel() throws Exception {
        String result = formatSql("drop model if exists rank_model");
        assertEquals("DROP MODEL IF EXISTS `rank_model`", result);
    }

    @Test
    void testDropService() throws Exception {
        String result = formatSql("drop service if exists rank_service");
        assertEquals("DROP SERVICE IF EXISTS `rank_service`", result);
    }

    @Test
    void testDropApi() throws Exception {
        String result = formatSql("drop api if exists main_rec");
        assertEquals("DROP API IF EXISTS `main_rec`", result);
    }

    @Test
    void testDropSqlFunction() throws Exception {
        String result = formatSql("drop sql function if exists recall_fun");
        assertEquals("DROP SQL FUNCTION IF EXISTS `recall_fun`", result);
    }

    @Test
    void testShowModels() throws Exception {
        String result = formatSql("show models");
        assertEquals("SHOW MODELS", result);
    }

    @Test
    void testShowServices() throws Exception {
        String result = formatSql("show services");
        assertEquals("SHOW SERVICES", result);
    }

    @Test
    void testShowCheckpoints() throws Exception {
        String result = formatSql("show checkpoints rank_model");
        assertEquals("SHOW CHECKPOINTS `rank_model`", result);
    }

    @Test
    void testAlterModelDropCheckpoint() throws Exception {
        String result = formatSql("alter model rank_model drop if exists checkpoint='v1'");
        assertEquals("ALTER MODEL `rank_model` DROP IF EXISTS CHECKPOINT = 'v1'", result);
    }
}
