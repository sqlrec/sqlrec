package com.sqlrec.connectors;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.utils.DataCheckUtils;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.connectors.redis.calcite.RedisCalciteTable;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.utils.SqlTestCase;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestComplexJoinRedis {
    private final String namespace = "join_test_" + UUID.randomUUID();
    private CalciteSchema schema;

    @BeforeAll
    void setUp() throws Exception {
        var tables = Map.of(
                "users", redisTable("users", "json"),
                "items", redisTable("items", "list"),
                "details", redisTable("details", "json"));
        schema = CalciteSchema.createRootSchema(false);
        var database = schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        tables.forEach(database::add);

        execute("insert into users values (1, 'Alice', 10), (2, 'Bob', 20)");
        execute("insert into items values (10, 'ItemA', 100), (10, 'ItemB', 200), (20, 'ItemC', 300)");
        execute("insert into details values (100, 'DetailA', 1), (200, 'DetailB', 2)");
        execute("cache table requests as select 1 as id union all select 2 union all select 3 "
                + "union all select cast(null as integer)");
    }

    @AfterAll
    void cleanUp() throws Exception {
        if (schema == null) {
            return;
        }
        for (var entry : Map.of("users", List.of(1, 2), "items", List.of(10, 20),
                "details", List.of(100, 200)).entrySet()) {
            for (int id : entry.getValue()) {
                execute("delete from " + entry.getKey() + " where id = " + id);
            }
        }
    }

    @Test
    void consecutiveInnerJoinsUseKeysReturnedByPreviousKvTables() throws Exception {
        assertQuery("""
                select q.id, u.name, i.name, d.name
                from requests q
                join users u on q.id = u.id
                join items i on u.cnt = i.id
                join details d on i.cnt = d.id
                order by q.id, i.name
                """, List.of(
                new Object[]{1, "Alice", "ItemA", "DetailA"},
                new Object[]{1, "Alice", "ItemB", "DetailB"}), 3);
    }

    @Test
    void consecutiveInnerAndLeftJoinsPreserveUnmatchedLastLookup() throws Exception {
        assertQuery("""
                select q.id, u.name, i.name, d.name
                from requests q
                join users u on q.id = u.id
                join items i on u.cnt = i.id
                left join details d on i.cnt = d.id
                order by q.id, i.name
                """, List.of(
                new Object[]{1, "Alice", "ItemA", "DetailA"},
                new Object[]{1, "Alice", "ItemB", "DetailB"},
                new Object[]{2, "Bob", "ItemC", null}), 3);
    }

    @Test
    void consecutiveLeftJoinsPreserveMissingAndNullKeys() throws Exception {
        assertQuery("""
                select q.id, u.name, i.name, d.name
                from requests q
                left join users u on q.id = u.id
                left join items i on u.cnt = i.id
                left join details d on i.cnt = d.id
                order by q.id nulls last, i.name
                """, List.of(
                new Object[]{1, "Alice", "ItemA", "DetailA"},
                new Object[]{1, "Alice", "ItemB", "DetailB"},
                new Object[]{2, "Bob", "ItemC", null},
                new Object[]{3, null, null, null},
                new Object[]{null, null, null, null}), 3);
    }

    @Test
    void sameKvTableCanBeJoinedThroughMultipleAliases() throws Exception {
        assertQuery("""
                select q.id, u.name, other.name
                from requests q
                join users u on q.id = u.id
                join users other on u.id = other.id
                order by q.id
                """, List.of(
                new Object[]{1, "Alice", "Alice"},
                new Object[]{2, "Bob", "Bob"}), 2);
    }

    @Test
    void leftNestedJoinPreservesProjectionAndFilter() throws Exception {
        assertQuery("""
                select x.request_id, x.user_name, i.name, d.name
                from (
                    select u.cnt as item_id, q.id as request_id, u.name as user_name
                    from requests q join users u on q.id = u.id
                    where q.id = 1
                ) x
                join items i on x.item_id = i.id
                join details d on i.cnt = d.id
                order by x.request_id, i.name
                """, List.of(
                new Object[]{1, "Alice", "ItemA", "DetailA"},
                new Object[]{1, "Alice", "ItemB", "DetailB"}), 3);
    }

    @Test
    void leftNestedLimitRunsBeforeOuterKvJoin() throws Exception {
        // ItemC has no detail. Limiting after the outer JOIN would incorrectly retain ItemA too.
        assertQuery("""
                select x.request_id, x.item_name, d.name
                from (
                    select i.cnt as detail_id, q.id as request_id, i.name as item_name
                    from requests q
                    join users u on q.id = u.id
                    join items i on u.cnt = i.id
                    order by i.name desc
                    limit 2
                ) x
                join details d on x.detail_id = d.id
                order by x.request_id, x.item_name
                """, List.<Object[]>of(new Object[]{1, "ItemB", "DetailB"}), 3);
    }

    @Test
    void multipleLeftNestedQueriesCanFeedKvJoin() throws Exception {
        assertQuery("""
                select x.request_id, x.item_name, d.name
                from (
                    select y.detail_id, y.request_id, y.item_name
                    from (
                        select q.id as request_id, i.name as item_name, i.cnt as detail_id
                        from requests q
                        join users u on q.id = u.id
                        join items i on u.cnt = i.id
                    ) y
                    where y.request_id = 1
                ) x
                left join details d on x.detail_id = d.id
                order by x.request_id, x.item_name
                """, List.of(
                new Object[]{1, "ItemA", "DetailA"},
                new Object[]{1, "ItemB", "DetailB"}), 3);
    }

    @Test
    @SilenceLoggers(SqlRecKvTable.class)
    void leftNestedFilterOnKvValueCurrentlyRequiresUnsupportedScan() {
        // Record the current boundary: optimization makes items the driving table.
        // Replace this error assertion with result assertions when that SQL shape is supported.
        var failure = assertThrows(UnsupportedOperationException.class, () -> assertQuery("""
                select x.request_id, x.item_name, d.name
                from (
                    select y.detail_id, y.request_id, y.item_name
                    from (
                        select q.id as request_id, i.name as item_name, i.cnt as detail_id
                        from requests q
                        join users u on q.id = u.id
                        join items i on u.cnt = i.id
                    ) y
                    where y.detail_id >= 200
                ) x
                left join details d on x.detail_id = d.id
                order by x.request_id, x.item_name
                """, List.of(
                new Object[]{1, "ItemB", "DetailB"},
                new Object[]{2, "ItemC", null}), 3));
        assertEquals("scan is not support by redis", failure.getMessage());
    }

    @Test
    void emptyLeftNestedJoinProducesNoRows() throws Exception {
        assertQuery("""
                select x.id, d.name
                from (
                    select q.id, u.cnt as detail_id
                    from requests q join users u on q.id = u.id
                    where q.id = -1
                ) x
                left join details d on x.detail_id = d.id
                """, List.of(), 2);
    }

    private RedisCalciteTable redisTable(String name, String dataStructure) {
        RedisConfig config = new RedisConfig();
        config.url = "redis://" + SqlRecConfigs.DEFAULT_TEST_IP.getValue() + ":30017/0";
        config.redisMode = "single";
        config.dataStructure = dataStructure;
        config.database = namespace;
        config.tableName = name;
        config.fieldSchemas = List.of(new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"), new FieldSchema("cnt", "INTEGER"));
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        config.ttl = 300;
        config.maxListSize = 10;
        config.cacheTtl = 0;
        config.maxCacheSize = 0;
        return new RedisCalciteTable(config);
    }

    private void execute(String sql) throws Exception {
        new SqlTestCase(sql, null).test(schema);
    }

    private void assertQuery(String sql, List<Object[]> expected, int lookupCount) throws Exception {
        var bindable = NormalSqlCompiler.getNormalSqlBindable(sql, schema, Consts.DEFAULT_SCHEMA_NAME);
        assertEquals(lookupCount, bindable.getPhysicalPlan().lines()
                .filter(line -> line.contains("SqlrecEnumerableKvJoin(")).count(), bindable.getPhysicalPlan());
        var rows = bindable.bind(schema, new ExecuteContextImpl(schema)).toList();
        DataCheckUtils.checkResultEqual(rows, expected);
    }
}
