package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.RuleDiversity;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleDiversityTest {
    private RuleDiversity greedy;

    @BeforeEach
    public void setUp() {
        greedy = new RuleDiversity();
    }

    // ==================== Basic Tests ====================

    @Test
    public void testEmptyTargetTable() {
        CacheTable targetTable = createTargetTable(new Object[][]{});
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {6, 1, 1, "category", "A", ">", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "10");
        List<Object[]> result = collectRows(output);
        assertEquals(0, result.size());
    }

    @Test
    public void testEmptyRuleTable() {
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"},
                {"item2", "B"},
                {"item3", "A"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{});

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "10");
        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        // Without rules, greedy preserves original order
        assertEquals("item1", result.get(0)[0]);
        assertEquals("item2", result.get(1)[0]);
        assertEquals("item3", result.get(2)[0]);
    }

    @Test
    public void testMaxReturnLimitsOutput() {
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"},
                {"item2", "B"},
                {"item3", "A"},
                {"item4", "B"},
                {"item5", "A"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{});

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "3");
        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
    }

    @Test
    public void testMaxReturnExceedsItemCount() {
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"},
                {"item2", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{});

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "100");
        List<Object[]> result = collectRows(output);
        assertEquals(2, result.size());
    }

    // ==================== Rule Constraint Tests ====================

    @Test
    public void testLessThanRule() {
        // Original order: A,A,A,A,B,B,B,B — first 4 positions have 4 A's (violates A < 3)
        // After diversification: first 4 positions should have at most 2 A's
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "A"},
                {"item5", "B"}, {"item6", "B"}, {"item7", "B"}, {"item8", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Verify constraint: first 4 positions have at most 2 A's
        long countAFirst4 = result.stream()
                .limit(4)
                .filter(r -> "A".equals(r[1]))
                .count();
        assertTrue(countAFirst4 <= 2,
                "First 4 positions should have at most 2 A's, got " + countAFirst4);
    }

    @Test
    public void testGreaterThanRule() {
        // Original order: B,B,B,A,A,A — first 4 positions have only 1 A (violates A > 2)
        // After diversification: first 4 positions should have at least 3 A's
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "B"}, {"item2", "B"}, {"item3", "B"},
                {"item4", "A"}, {"item5", "A"}, {"item6", "A"},
                {"item7", "B"}, {"item8", "A"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", ">", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Verify constraint: first 4 positions have at least 3 A's
        long countAFirst4 = result.stream()
                .limit(4)
                .filter(r -> "A".equals(r[1]))
                .count();
        assertTrue(countAFirst4 >= 3,
                "First 4 positions should have at least 3 A's, got " + countAFirst4);
    }

    @Test
    public void testEqualRule() {
        // Original order: A,A,A,B,A,B — first 4 positions have 3 A's (violates A = 2)
        // After diversification: first 4 positions should have exactly 2 A's
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "B"},
                {"item5", "A"}, {"item6", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", "=", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "6");
        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());

        long countA = result.stream()
                .limit(4)
                .filter(r -> "A".equals(r[1]))
                .count();
        assertEquals(2, countA, "Should have exactly 2 A's in first 4 positions");
    }

    @Test
    public void testSlidingWindow() {
        // Original order: 5 A's then 5 B's — every window of 4 starting at pos 0,1,2 violates A < 3
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "A"}, {"item5", "A"},
                {"item6", "B"}, {"item7", "B"}, {"item8", "B"}, {"item9", "B"}, {"item10", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 3, "category", "A", "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "10");
        List<Object[]> result = collectRows(output);
        assertEquals(10, result.size());

        // Verify ALL sliding windows satisfy the constraint
        for (int w = 0; w < 3; w++) {
            long countA = result.stream()
                    .skip(w)
                    .limit(4)
                    .filter(r -> "A".equals(r[1]))
                    .count();
            assertTrue(countA <= 2,
                    "Window starting at position " + w + " should have at most 2 A's, got " + countA);
        }
    }

    @Test
    public void testDiversityValueNullEachValueConstrained() {
        // Original order: A,A,A,B,B,B,C,C — first 6 positions have 3 A's and 3 B's (violates each < 3)
        // Rule: null diversity_value, each value < 3 in window of 6
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"},
                {"item4", "B"}, {"item5", "B"}, {"item6", "B"},
                {"item7", "C"}, {"item8", "C"}, {"item9", "D"}
        });
        CacheTable ruleTable = createRuleTableWithNullValue(new Object[][]{
                {6, 1, 1, "category", null, "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "9");
        List<Object[]> result = collectRows(output);
        assertEquals(9, result.size());

        // Verify: in first 6 positions, no category appears 3+ times
        Map<String, Long> counts = new java.util.HashMap<>();
        for (int i = 0; i < 6; i++) {
            String cat = (String) result.get(i)[1];
            counts.merge(cat, 1L, Long::sum);
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            assertTrue(e.getValue() < 3,
                    "Category " + e.getKey() + " appears " + e.getValue() + " times in first 6, expected < 3");
        }
    }

    @Test
    public void testDiversityValueNullSkipsNullAttribute() {
        // Items with null attribute value should not be counted for null diversity_value rules
        // Original order: A,A,A,B,B,B — first 6 positions have 3 A's and 3 B's (violates each < 3)
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"},
                {"item4", "B"}, {"item5", "B"}, {"item6", "B"},
                {"item7", null}, {"item8", null}
        });
        CacheTable ruleTable = createRuleTableWithNullValue(new Object[][]{
                {6, 1, 1, "category", null, "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Verify: in first 6 positions, each non-null category appears < 3 times
        Map<String, Long> counts = new java.util.HashMap<>();
        for (int i = 0; i < 6; i++) {
            Object cat = result.get(i)[1];
            if (cat != null) {
                counts.merge(cat.toString(), 1L, Long::sum);
            }
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            assertTrue(e.getValue() < 3,
                    "Category " + e.getKey() + " appears " + e.getValue() + " times, expected < 3");
        }
    }

    @Test
    public void testUnknownOpSkipped() {
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"},
                {"item2", "B"},
                {"item3", "A"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {3, 1, 1, "category", "A", "unknown_op", 1, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "3");
        List<Object[]> result = collectRows(output);
        assertEquals(3, result.size());
        // Unknown op skipped, greedy preserves original order
        assertEquals("item1", result.get(0)[0]);
        assertEquals("item2", result.get(1)[0]);
        assertEquals("item3", result.get(2)[0]);
    }

    @Test
    public void testOpAliases() {
        // Original order: B,B,B,A,A,A — first 6 positions have only 3 A's (violates A > 3, i.e. >= 4)
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "B"}, {"item2", "B"}, {"item3", "B"},
                {"item4", "A"}, {"item5", "A"}, {"item6", "A"},
                {"item7", "A"}, {"item8", "A"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {6, 1, 1, "category", "A", "gt", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        long countA = result.stream()
                .limit(6)
                .filter(r -> "A".equals(r[1]))
                .count();
        assertTrue(countA >= 4, "gt should be normalized to >, so at least 4 A's in first 6, got " + countA);
    }

    @Test
    public void testConflictingRulesWeightPriority() {
        // 4 A's and 4 B's. Rules: A > 2 (weight=10) AND B > 2 (weight=1) in window of 4.
        // Both can't be satisfied simultaneously (only 4 slots, need 3+ A and 3+ B = 6+ items).
        // Higher weight rule (A > 2) should be prioritized.
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "A"},
                {"item5", "B"}, {"item6", "B"}, {"item7", "B"}, {"item8", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", ">", 2, 10.0},
                {4, 1, 1, "category", "B", ">", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Higher weight rule should be better satisfied
        long countAFirst4 = result.stream().limit(4).filter(r -> "A".equals(r[1])).count();
        long countBFirst4 = result.stream().limit(4).filter(r -> "B".equals(r[1])).count();
        // A's rule has higher weight, so A should be closer to > 2 (i.e. >= 3) than B
        assertTrue(countAFirst4 >= countBFirst4,
                "Higher weight rule should be better satisfied: A count=" + countAFirst4 + ", B count=" + countBFirst4);
        assertTrue(countBFirst4 >= 1, "");
    }

    @Test
    public void testListCategoryValues() {
        // Items with list-type category values
        // Original order: items 1-3 all contain "A", items 4-6 don't
        // Rule: A < 2 in window of 4 — original first 4 would have 3 items with A (violates)
        CacheTable targetTable = createTargetTableWithListCategory(new Object[][]{
                {"item1", Arrays.asList("A", "B")},
                {"item2", Arrays.asList("A", "C")},
                {"item3", Arrays.asList("A", "D")},
                {"item4", Arrays.asList("B", "C")},
                {"item5", Arrays.asList("B", "D")},
                {"item6", Arrays.asList("C", "D")},
                {"item7", Arrays.asList("E")},
                {"item8", Arrays.asList("E", "F")}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", "<", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Verify: at most 1 item with "A" in first 4 positions
        long countA = result.stream()
                .limit(4)
                .filter(r -> {
                    @SuppressWarnings("unchecked")
                    List<String> cats = (List<String>) r[1];
                    return cats.contains("A");
                })
                .count();
        assertTrue(countA <= 1, "Should have at most 1 item with A in first 4 positions, got " + countA);
    }

    @Test
    public void testListCategoryWithNullDiversityValue() {
        // Items with list-type category values, null diversity_value
        // Original order: first 3 items all have A — violates each value < 3 in window of 4
        CacheTable targetTable = createTargetTableWithListCategory(new Object[][]{
                {"item1", Arrays.asList("A", "B")},
                {"item2", Arrays.asList("A", "C")},
                {"item3", Arrays.asList("A", "D")},
                {"item4", Arrays.asList("B", "C")},
                {"item5", Arrays.asList("E")},
                {"item6", Arrays.asList("F")}
        });
        CacheTable ruleTable = createRuleTableWithNullValue(new Object[][]{
                {4, 1, 1, "category", null, "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "6");
        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());

        // Verify: in first 4 positions, each value appears < 3 times
        Map<String, Long> counts = new java.util.HashMap<>();
        for (int i = 0; i < 4; i++) {
            @SuppressWarnings("unchecked")
            List<String> cats = (List<String>) result.get(i)[1];
            for (String cat : cats) {
                counts.merge(cat, 1L, Long::sum);
            }
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            assertTrue(e.getValue() < 3,
                    "Value " + e.getKey() + " appears " + e.getValue() + " times in first 4, expected < 3");
        }
    }

    @Test
    public void testOriginalOrderPreservedWhenNoViolation() {
        // When original order already satisfies all rules, it should be preserved
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "B"}, {"item3", "C"},
                {"item4", "A"}, {"item5", "B"}, {"item6", "C"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {3, 1, 1, "category", "A", "<", 2, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "6");
        List<Object[]> result = collectRows(output);
        assertEquals(6, result.size());

        // Original order already satisfies A < 2 in any window of 3, so it should be preserved
        for (int i = 0; i < 6; i++) {
            assertEquals("item" + (i + 1), result.get(i)[0],
                    "Original order should be preserved when no violation");
        }
    }

    @Test
    public void testMultipleRulesSimultaneously() {
        // Original order: 6 A's then 6 B's
        // Rule 1: A < 3 in window of 4 (window_start=1, window_num=1)
        // Rule 2: B < 3 in window of 4 (window_start=1, window_num=1)
        // Only the first window (positions 0-3) is constrained
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "A"}, {"item5", "A"}, {"item6", "A"},
                {"item7", "B"}, {"item8", "B"}, {"item9", "B"}, {"item10", "B"}, {"item11", "B"}, {"item12", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 1, 1, "category", "A", "<", 3, 1.0},
                {4, 1, 1, "category", "B", "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "12");
        List<Object[]> result = collectRows(output);
        assertEquals(12, result.size());

        // Verify: first 4 positions (the constrained window) have < 3 A's and < 3 B's
        long countA = 0, countB = 0;
        for (int i = 0; i < 4; i++) {
            if ("A".equals(result.get(i)[1])) countA++;
            if ("B".equals(result.get(i)[1])) countB++;
        }
        assertTrue(countA < 3, "First 4 positions should have < 3 A's, got " + countA);
        assertTrue(countB < 3, "First 4 positions should have < 3 B's, got " + countB);

        // Verify diversification happened: original first 4 had 4 A's
        assertTrue(countA < 4, "Diversification should have reduced A's in first 4 positions");
    }

    @Test
    public void testWindowStartNotAtOne() {
        // Rule starts at position 3 (1-based), so positions 0-1 are unconstrained
        // Original order: A,A,A,A,B,B — positions 2-5 have 2 A's and 2 B's (satisfies A < 3)
        // But if we put A,A at positions 0-1, then positions 2-5 need A < 3
        CacheTable targetTable = createTargetTable(new Object[][]{
                {"item1", "A"}, {"item2", "A"}, {"item3", "A"}, {"item4", "A"},
                {"item5", "B"}, {"item6", "B"}, {"item7", "B"}, {"item8", "B"}
        });
        CacheTable ruleTable = createRuleTable(new Object[][]{
                {4, 3, 1, "category", "A", "<", 3, 1.0}
        });

        CacheTable output = greedy.evaluate(targetTable, ruleTable, "8");
        List<Object[]> result = collectRows(output);
        assertEquals(8, result.size());

        // Verify: positions 2-5 (0-based) have at most 2 A's
        long countA = 0;
        for (int i = 2; i < 6; i++) {
            if ("A".equals(result.get(i)[1])) countA++;
        }
        assertTrue(countA <= 2,
                "Window starting at position 3 (1-based) should have at most 2 A's, got " + countA);
    }

    // ==================== Performance Benchmark ====================

    @Test
    public void testPerformanceBenchmark() {
        int warmupRuns = 1;
        int measureRuns = 1;

        Object[][] configs = {
                // {name, n, maxReturn, numCategories, windowSize, windowNum, rulesPerCategory}
                {"100 items, 10 rules", 100, 10, 5, 6, 3, 2},
                {"300 items, 20 rules", 300, 10, 5, 6, 5, 2},
                {"500 items, 20 rules", 500, 10, 5, 6, 5, 2},
                {"1K items, 30 rules", 1000, 10, 5, 6, 5, 2},
                {"1K items, 50 rules", 1000, 20, 8, 6, 8, 3},
        };

        System.out.println("=== RuleDiversityGreedy Performance Benchmark ===");
        System.out.printf("%-25s %8s %8s %8s %12s %12s%n",
                "Config", "Items", "Rules", "MaxRet", "Avg(ms)", "Min(ms)");
        System.out.println("-".repeat(75));

        for (Object[] cfg : configs) {
            String name = (String) cfg[0];
            int n = (int) cfg[1];
            int maxReturn = (int) cfg[2];
            int numCategories = (int) cfg[3];
            int windowSize = (int) cfg[4];
            int windowNum = (int) cfg[5];
            int rulesPerCategory = (int) cfg[6];

            String[] categories = new String[numCategories];
            for (int i = 0; i < numCategories; i++) {
                categories[i] = String.valueOf((char) ('A' + i));
            }

            Object[][] targetData = new Object[n][];
            Random rand = new Random(42);
            for (int i = 0; i < n; i++) {
                targetData[i] = new Object[]{"item" + i, categories[i % numCategories]};
            }
            CacheTable targetTable = createTargetTable(targetData);

            List<Object[]> ruleList = new ArrayList<>();
            for (String cat : categories) {
                ruleList.add(new Object[]{windowSize, 1, windowNum, "category", cat, "<", 3, 2.0});
                if (rulesPerCategory > 1) {
                    ruleList.add(new Object[]{windowSize - 2, 1, windowNum + 2, "category", cat, ">", 0, 1.0});
                }
            }
            CacheTable ruleTable = createRuleTable(ruleList.toArray(new Object[0][]));

            // Warmup
            for (int r = 0; r < warmupRuns; r++) {
                greedy.evaluate(targetTable, ruleTable, String.valueOf(maxReturn));
            }

            // Measure
            long[] times = new long[measureRuns];
            for (int r = 0; r < measureRuns; r++) {
                long start = System.nanoTime();
                CacheTable output = greedy.evaluate(targetTable, ruleTable, String.valueOf(maxReturn));
                times[r] = System.nanoTime() - start;

                if (r == 0) {
                    List<Object[]> result = collectRows(output);
                    assertEquals(maxReturn, result.size());
                }
            }

            long avgNs = Arrays.stream(times).sum() / measureRuns;
            long minNs = Arrays.stream(times).min().orElse(0);
            double avgMs = avgNs / 1_000_000.0;
            double minMs = minNs / 1_000_000.0;

            System.out.printf("%-25s %8d %8d %8d %12.2f %12.2f%n",
                    name, n, ruleList.size(), maxReturn, avgMs, minMs);
        }
    }

    // ==================== Helper Methods ====================

    private CacheTable createTargetTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("target", Linq4j.asEnumerable(rows), createTargetFields());
    }

    private List<RelDataTypeField> createTargetFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("category", 1, SqlTypeName.VARCHAR));
        return fields;
    }

    private CacheTable createRuleTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("rules", Linq4j.asEnumerable(rows), createRuleFields());
    }

    private CacheTable createRuleTableWithNullValue(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("rules", Linq4j.asEnumerable(rows), createRuleFields());
    }

    private CacheTable createTargetTableWithListCategory(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("target", Linq4j.asEnumerable(rows), createTargetFieldsWithListCategory());
    }

    private List<RelDataTypeField> createTargetFieldsWithListCategory() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("category", 1, SqlTypeName.ARRAY));
        return fields;
    }

    private List<RelDataTypeField> createRuleFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("window_size", 0, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("window_start", 1, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("window_num", 2, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("diversity_column", 3, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("diversity_value", 4, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("op", 5, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("diversity_num", 6, SqlTypeName.INTEGER));
        fields.add(DataTypeUtils.getRelDataTypeField("weight", 7, SqlTypeName.DOUBLE));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
