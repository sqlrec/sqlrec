package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Rule-based diversity UDF using greedy algorithm.
 * <p>
 * Inputs:
 * - targetTable: items to be diversified/reordered
 * - ruleTable: diversity rules
 * - maxReturn: maximum number of items to return
 * <p>
 * Rule table fields:
 * - window_size: window size
 * - window_start: window start position (1-based)
 * - window_num: number of sliding windows (1 = no sliding)
 * - diversity_column: column name in target table to diversify on
 * - diversity_value: value to match (null/empty = constraint applies to each distinct value)
 * - op: comparison operator (">", "=", "<")
 * - diversity_num: threshold for the constraint
 * - weight: rule weight (float), higher weight = higher priority
 * <p>
 * The diversity column may contain a single value or a list of values.
 * When it's a list, each value in the list is treated as a separate attribute.
 * <p>
 * When diversity_value is null/empty, the constraint applies to each distinct attribute value
 * in the window: every distinct value's count must satisfy the op/diversity_num constraint.
 * <p>
 * Algorithm: greedy position-by-position assignment.
 * For each output position j (0 to maxReturn-1), select the unassigned item i
 * that minimizes violation penalty. If no violation, use original rank as tie-breaker.
 */
public class RuleDiversity {

    private static final String COL_WINDOW_SIZE = "window_size";
    private static final String COL_WINDOW_START = "window_start";
    private static final String COL_WINDOW_NUM = "window_num";
    private static final String COL_DIVERSITY_COLUMN = "diversity_column";
    private static final String COL_DIVERSITY_VALUE = "diversity_value";
    private static final String COL_OP = "op";
    private static final String COL_DIVERSITY_NUM = "diversity_num";
    private static final String COL_WEIGHT = "weight";

    // Penalty multiplier for rule violations, must dominate (i+1) so that
    // constraint satisfaction is prioritized over original order preservation.
    private static final double VIOLATION_PENALTY = 1_000_000.0;

    public CacheTable evaluate(
            CacheTable targetTable,
            CacheTable ruleTable,
            String maxReturn
    ) {
        int maxReturnVal = Integer.parseInt(maxReturn);

        List<Object[]> targetRows = readRows(targetTable);
        int n = targetRows.size();
        if (n == 0 || maxReturnVal <= 0) {
            return new CacheTable(
                    targetTable.getTableName() + "_rule_diversify_greedy",
                    Linq4j.asEnumerable(new ArrayList<>()),
                    targetTable.getDataFields()
            );
        }
        maxReturnVal = Math.min(maxReturnVal, n);

        List<Rule> rules = parseRules(ruleTable, targetTable.getDataFields());

        // Pre-extract item values: only columns referenced by rules
        List<Map<Integer, List<String>>> itemValues = extractItemValues(targetRows, n, rules);

        // Build all windows (each window holds a reference to its rule)
        List<WindowInfo> allWindows = buildWindows(rules, maxReturnVal);

        // Pre-compute: for each position, which windows are active
        List<List<WindowInfo>> windowsPerPosition = buildWindowsPerPosition(allWindows, maxReturnVal);

        // Greedy assignment
        int[] result = greedyAssign(n, maxReturnVal, itemValues, windowsPerPosition);

        // Build result rows
        List<Object[]> resultRows = new ArrayList<>();
        for (int j = 0; j < maxReturnVal; j++) {
            if (result[j] >= 0) {
                resultRows.add(targetRows.get(result[j]));
            }
        }

        return new CacheTable(
                targetTable.getTableName() + "_rule_diversify_greedy",
                Linq4j.asEnumerable(resultRows),
                targetTable.getDataFields()
        );
    }

    private static List<Object[]> readRows(CacheTable table) {
        List<Object[]> rows = new ArrayList<>();
        Enumerable<Object[]> enumerable = table.scan(null);
        if (enumerable != null) {
            for (Object[] row : enumerable) {
                rows.add(row);
            }
        }
        return rows;
    }

    private static List<Rule> parseRules(CacheTable ruleTable, List<RelDataTypeField> targetFields) {
        List<RelDataTypeField> ruleFields = ruleTable.getDataFields();
        int windowSizeIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_WINDOW_SIZE);
        int windowStartIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_WINDOW_START);
        int windowNumIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_WINDOW_NUM);
        int diversityColumnIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_DIVERSITY_COLUMN);
        int diversityValueIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_DIVERSITY_VALUE);
        int opIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_OP);
        int diversityNumIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_DIVERSITY_NUM);
        int weightIdx = DataTypeUtils.findFieldIndex(ruleFields, COL_WEIGHT);

        if (windowSizeIdx == -1) throw new IllegalArgumentException("rule table missing column: " + COL_WINDOW_SIZE);
        if (windowStartIdx == -1) throw new IllegalArgumentException("rule table missing column: " + COL_WINDOW_START);
        if (windowNumIdx == -1) throw new IllegalArgumentException("rule table missing column: " + COL_WINDOW_NUM);
        if (diversityColumnIdx == -1)
            throw new IllegalArgumentException("rule table missing column: " + COL_DIVERSITY_COLUMN);
        if (opIdx == -1) throw new IllegalArgumentException("rule table missing column: " + COL_OP);
        if (diversityNumIdx == -1)
            throw new IllegalArgumentException("rule table missing column: " + COL_DIVERSITY_NUM);
        if (weightIdx == -1) throw new IllegalArgumentException("rule table missing column: " + COL_WEIGHT);

        List<Rule> rules = new ArrayList<>();
        Enumerable<Object[]> ruleEnum = ruleTable.scan(null);
        if (ruleEnum != null) {
            for (Object[] row : ruleEnum) {
                rules.add(Rule.fromRow(row, windowSizeIdx, windowStartIdx, windowNumIdx,
                        diversityColumnIdx, diversityValueIdx, opIdx, diversityNumIdx, weightIdx, targetFields));
            }
        }
        return rules;
    }

    /**
     * Extract item values for columns referenced by rules.
     * Each row is represented as a Map: columnIndex -> list of string values.
     */
    private static List<Map<Integer, List<String>>> extractItemValues(
            List<Object[]> targetRows, int n, List<Rule> rules) {
        // Collect unique column indices used by rules
        Set<Integer> usedColumns = new HashSet<>();
        for (Rule rule : rules) {
            usedColumns.add(rule.columnIndex);
        }

        List<Map<Integer, List<String>>> itemValues = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Object[] row = targetRows.get(i);
            Map<Integer, List<String>> rowMap = new HashMap<>();
            for (int colIdx : usedColumns) {
                if (colIdx < row.length) {
                    rowMap.put(colIdx, toValueList(row[colIdx]));
                } else {
                    rowMap.put(colIdx, Collections.emptyList());
                }
            }
            itemValues.add(rowMap);
        }
        return itemValues;
    }

    /**
     * Build all windows for all rules. Each window holds a reference to its rule.
     */
    private static List<WindowInfo> buildWindows(List<Rule> rules, int maxReturnVal) {
        List<WindowInfo> allWindows = new ArrayList<>();
        for (Rule rule : rules) {
            if (!rule.isValidOp()) continue;
            for (int w = 0; w < rule.windowNum; w++) {
                int winStart = rule.windowStart - 1 + w;
                int winEnd = Math.min(winStart + rule.windowSize, maxReturnVal);
                if (winStart >= maxReturnVal) break;
                allWindows.add(new WindowInfo(winStart, winEnd, rule));
            }
        }
        return allWindows;
    }

    /**
     * Build per-position window lists.
     */
    private static List<List<WindowInfo>> buildWindowsPerPosition(List<WindowInfo> allWindows, int maxReturnVal) {
        List<List<WindowInfo>> windowsPerPosition = new ArrayList<>();
        for (int j = 0; j < maxReturnVal; j++) {
            List<WindowInfo> active = new ArrayList<>();
            for (WindowInfo win : allWindows) {
                if (win.covers(j)) {
                    active.add(win);
                }
            }
            windowsPerPosition.add(active);
        }
        return windowsPerPosition;
    }

    private static int[] greedyAssign(int n, int maxReturnVal,
                                      List<Map<Integer, List<String>>> itemValues,
                                      List<List<WindowInfo>> windowsPerPosition) {
        boolean[] assigned = new boolean[n];
        int[] result = new int[maxReturnVal];
        Arrays.fill(result, -1);

        for (int j = 0; j < maxReturnVal; j++) {
            int bestItem = -1;
            double bestScore = Double.MAX_VALUE;
            List<WindowInfo> activeWindows = windowsPerPosition.get(j);

            for (int i = 0; i < n; i++) {
                if (assigned[i]) continue;

                double violation = computeViolation(i, activeWindows, itemValues.get(i));

                // Short-circuit: zero violation + iterating in order = best possible
                if (violation == 0) {
                    bestItem = i;
                    break;
                }

                double score = violation * VIOLATION_PENALTY + (i + 1);
                if (score < bestScore) {
                    bestScore = score;
                    bestItem = i;
                }
            }

            if (bestItem == -1) {
                for (int i = 0; i < n; i++) {
                    if (!assigned[i]) {
                        bestItem = i;
                        break;
                    }
                }
                if (bestItem == -1) break;
            }

            result[j] = bestItem;
            assigned[bestItem] = true;
            updateWindowCounts(bestItem, activeWindows, itemValues.get(bestItem));
        }
        return result;
    }

    private static double computeViolation(int itemIdx, List<WindowInfo> activeWindows,
                                           Map<Integer, List<String>> itemRow) {
        double violation = 0;
        for (WindowInfo win : activeWindows) {
            violation += win.simulateViolation(itemRow, itemIdx);
        }
        return violation;
    }

    private static void updateWindowCounts(int itemIdx, List<WindowInfo> activeWindows,
                                           Map<Integer, List<String>> itemRow) {
        for (WindowInfo win : activeWindows) {
            win.add(itemRow, itemIdx);
        }
    }

    /**
     * Convert a column value to a list of strings.
     * If the value is a List, each element is converted to string.
     * If the value is a single non-null object, it's wrapped in a singleton list.
     * If the value is null, returns an empty list.
     */
    private static List<String> toValueList(Object val) {
        if (val == null) {
            return Collections.emptyList();
        }
        if (val instanceof List) {
            return ((List<?>) val).stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(val.toString());
    }

    private static int toInt(Object val) {
        if (val == null) {
            throw new IllegalArgumentException("Expected non-null integer value");
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        return Integer.parseInt(val.toString());
    }

    private static double toDouble(Object val) {
        if (val == null) {
            return 0.0;
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return Double.parseDouble(val.toString());
    }

    private static String toString(Object val) {
        if (val == null) {
            return null;
        }
        return val.toString();
    }

    private static String normalizeOp(String op) {
        if (op == null) {
            return null;
        }
        switch (op.trim().toLowerCase()) {
            case ">":
            case "gt":
            case "gte":
            case "ge":
            case "greater":
                return Rule.OP_GT;
            case "=":
            case "eq":
            case "equal":
                return Rule.OP_EQ;
            case "<":
            case "lt":
            case "lte":
            case "le":
            case "less":
                return Rule.OP_LT;
            default:
                return op;
        }
    }

    // ==================== Inner Classes ====================

    private static class Rule {
        static final String OP_GT = ">";
        static final String OP_EQ = "=";
        static final String OP_LT = "<";

        int windowSize;
        int windowStart;
        int windowNum;
        String diversityColumn;
        String diversityValue;
        String op;
        int diversityNum;
        double weight;
        int columnIndex;
        boolean isNullValue;

        static Rule fromRow(Object[] row, int windowSizeIdx, int windowStartIdx, int windowNumIdx,
                            int diversityColumnIdx, int diversityValueIdx, int opIdx, int diversityNumIdx,
                            int weightIdx, List<RelDataTypeField> targetFields) {
            int maxIdx = Math.max(Math.max(Math.max(windowSizeIdx, windowStartIdx), windowNumIdx),
                    Math.max(Math.max(diversityColumnIdx, diversityValueIdx),
                            Math.max(Math.max(opIdx, diversityNumIdx), weightIdx)));
            if (row.length <= maxIdx) {
                throw new IllegalArgumentException("Rule row has " + row.length + " columns, expected at least " + (maxIdx + 1));
            }
            Rule rule = new Rule();
            rule.windowSize = RuleDiversity.toInt(row[windowSizeIdx]);
            rule.windowStart = RuleDiversity.toInt(row[windowStartIdx]);
            rule.windowNum = RuleDiversity.toInt(row[windowNumIdx]);
            rule.diversityColumn = RuleDiversity.toString(row[diversityColumnIdx]);
            rule.diversityValue = row[diversityValueIdx] == null ? null : RuleDiversity.toString(row[diversityValueIdx]);
            rule.op = RuleDiversity.normalizeOp(RuleDiversity.toString(row[opIdx]));
            rule.diversityNum = RuleDiversity.toInt(row[diversityNumIdx]);
            rule.weight = RuleDiversity.toDouble(row[weightIdx]);
            rule.isNullValue = (rule.diversityValue == null || rule.diversityValue.isEmpty());

            // Validate rule parameters
            if (rule.windowSize <= 0) {
                throw new IllegalArgumentException("window_size must be > 0, got " + rule.windowSize);
            }
            if (rule.windowStart < 1) {
                throw new IllegalArgumentException("window_start must be >= 1, got " + rule.windowStart);
            }
            if (rule.windowNum < 1) {
                throw new IllegalArgumentException("window_num must be >= 1, got " + rule.windowNum);
            }
            if (rule.diversityColumn == null || rule.diversityColumn.isEmpty()) {
                throw new IllegalArgumentException("diversity_column must not be null or empty");
            }
            if (rule.op == null) {
                throw new IllegalArgumentException("op must not be null");
            }
            if (rule.diversityNum < 0) {
                throw new IllegalArgumentException("diversity_num must be >= 0, got " + rule.diversityNum);
            }
            if (rule.weight < 0) {
                throw new IllegalArgumentException("weight must be >= 0, got " + rule.weight);
            }

            rule.columnIndex = DataTypeUtils.findFieldIndex(targetFields, rule.diversityColumn);
            if (rule.columnIndex == -1) {
                throw new IllegalArgumentException(
                        "diversity_column '" + rule.diversityColumn + "' not found in target table");
            }
            return rule;
        }

        boolean isValidOp() {
            return OP_GT.equals(op) || OP_LT.equals(op) || OP_EQ.equals(op);
        }

        double calcViolation(int count) {
            switch (op) {
                case OP_GT:
                    return Math.max(0, diversityNum + 1 - count);
                case OP_LT:
                    return Math.max(0, count - Math.max(0, diversityNum - 1));
                case OP_EQ:
                    return Math.abs(count - diversityNum);
                default:
                    return 0;
            }
        }
    }

    private static class WindowInfo {
        final int start;
        final int end;
        final Rule rule;
        int matchedCount;
        final Map<String, Integer> valueCounts;

        WindowInfo(int start, int end, Rule rule) {
            this.start = start;
            this.end = end;
            this.rule = rule;
            this.valueCounts = rule.isNullValue ? new HashMap<>() : null;
        }

        boolean covers(int position) {
            return position >= start && position < end;
        }

        /**
         * Simulate placing an item and compute the weighted violation penalty.
         */
        double simulateViolation(Map<Integer, List<String>> itemRow, int itemIdx) {
            List<String> values = itemRow.getOrDefault(rule.columnIndex, Collections.emptyList());

            if (rule.isNullValue) {
                if (values.isEmpty()) return 0;
                Map<String, Integer> newCounts = new HashMap<>(valueCounts);
                for (String val : values) {
                    newCounts.merge(val, 1, Integer::sum);
                }
                double maxViol = 0;
                for (int count : newCounts.values()) {
                    double v = rule.calcViolation(count);
                    if (v > maxViol) maxViol = v;
                }
                return maxViol * rule.weight;
            } else {
                boolean isMatched = values.contains(rule.diversityValue);
                int newCount = matchedCount + (isMatched ? 1 : 0);
                return rule.calcViolation(newCount) * rule.weight;
            }
        }

        /**
         * Update window counts after placing an item.
         */
        void add(Map<Integer, List<String>> itemRow, int itemIdx) {
            List<String> values = itemRow.getOrDefault(rule.columnIndex, Collections.emptyList());

            if (rule.isNullValue) {
                for (String val : values) {
                    valueCounts.merge(val, 1, Integer::sum);
                }
            } else {
                if (values.contains(rule.diversityValue)) {
                    matchedCount++;
                }
            }
        }
    }
}
