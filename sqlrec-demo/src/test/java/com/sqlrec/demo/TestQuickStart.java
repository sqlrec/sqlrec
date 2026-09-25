package com.sqlrec.demo;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.executor.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQuickStart {
    private static SqlExecutor sqlExecutor;

    @Test
    void testQuickStartFunction() throws Exception {
        sqlExecutor = new SqlExecutor();
        List<Object[]> firstUserInterests = sqlExecutor.executeSql(
                "select category from demo_user_interest_category where user_id = 1000001")
                .scan(null).toList();
        assertEquals(Set.of("pc", "phone", "book"),
                firstUserInterests.stream().map(row -> (String) row[0]).collect(Collectors.toSet()));
        List<Object[]> interests = sqlExecutor.executeSql("select * from demo_user_interest_category")
                .scan(null).toList();
        assertEquals(15, interests.size());
        assertEquals(Map.of(1000001L, 3L, 1000002L, 3L, 1000003L, 3L, 1000004L, 3L, 1000005L, 3L),
                interests.stream().collect(Collectors.groupingBy(row -> (Long) row[0], Collectors.counting())));
        assertEquals(Set.of("pc", "phone", "book", "sports", "home"),
                interests.stream().map(row -> (String) row[1]).collect(Collectors.toSet()));
        List<Object[]> items = sqlExecutor.executeSql("select * from demo_category_hot_item")
                .scan(null).toList();
        assertEquals(Map.of("pc", 5L, "phone", 5L, "book", 5L, "sports", 5L, "home", 5L),
                items.stream().collect(Collectors.groupingBy(row -> (String) row[0], Collectors.counting())));

        sqlExecutor.executeSql("cache table quick_start_user as select cast(1000001 as bigint) as user_id");

        CacheTable result = sqlExecutor.executeSql("call demo_rec(quick_start_user)");
        List<Object[]> rows = result.scan(null).toList();

        List<String> lines = DataTransformUtils.formatAsTable(result.scan(null), result.getDataFields());
        lines.forEach(System.out::println);

        assertEquals(2, rows.size());
        rows.forEach(row -> assertEquals(1000001L, row[0]));

        sqlExecutor.executeSql("""
                insert into demo_exposure_item values
                (1000002, 9999999, 1), (1000002, 9999998, 2),
                (1000003, 9999999, 3)
                """);
        List<Object[]> secondUserExposures = sqlExecutor.executeSql(
                "select item_id from demo_exposure_item where user_id = 1000002")
                .scan(null).toList();
        assertEquals(Set.of(9999999L, 9999998L),
                secondUserExposures.stream().map(row -> (Long) row[0]).collect(Collectors.toSet()));
        assertEquals(2, secondUserExposures.size());
        List<Object[]> sharedItemExposures = sqlExecutor.executeSql(
                "select user_id from demo_exposure_item where item_id = 9999999")
                .scan(null).toList();
        assertEquals(Set.of(1000002L, 1000003L),
                sharedItemExposures.stream().map(row -> (Long) row[0]).collect(Collectors.toSet()));
        assertEquals(2, sharedItemExposures.size());
    }
}
