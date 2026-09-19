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
public final class RuleDiversity {

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
        List<Object[]> targetRows = readRows(targetTable);
        int limit = Math.min(Integer.parseInt(maxReturn), targetRows.size());
        if (limit <= 0) {
            return resultTable(targetTable, Collections.emptyList());
        }

        List<Rule> rules = parseRules(ruleTable, targetTable.getDataFields());
        List<Map<Integer, List<String>>> itemValues = extractItemValues(targetRows, rules);
        List<WindowInfo> windows = buildWindows(rules, limit);
        List<List<WindowInfo>> windowsPerPosition = buildWindowsPerPosition(windows, limit);
        int[] selectedIndexes = greedyAssign(targetRows.size(), limit, itemValues, windowsPerPosition);

        List<Object[]> selectedRows = new ArrayList<>(limit);
        for (int selectedIndex : selectedIndexes) {
            selectedRows.add(targetRows.get(selectedIndex));
        }
        return resultTable(targetTable, selectedRows);
    }

    private static CacheTable resultTable(CacheTable source, List<Object[]> rows) {
        return new CacheTable(
                source.getTableName() + "_rule_diversify_greedy",
                Linq4j.asEnumerable(rows),
                source.getDataFields());
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
        RuleFields fields = RuleFields.from(ruleTable.getDataFields());
        List<Rule> rules = new ArrayList<>();
        Enumerable<Object[]> ruleEnum = ruleTable.scan(null);
        if (ruleEnum != null) {
            for (Object[] row : ruleEnum) {
                rules.add(Rule.fromRow(row, fields, targetFields));
            }
        }
        return rules;
    }

    /**
     * Extract item values for columns referenced by rules.
     * Each row is represented as a Map: columnIndex -> list of string values.
     */
    private static List<Map<Integer, List<String>>> extractItemValues(
            List<Object[]> targetRows, List<Rule> rules) {
        Set<Integer> usedColumns = new HashSet<>();
        for (Rule rule : rules) {
            usedColumns.add(rule.columnIndex);
        }

        List<Map<Integer, List<String>>> itemValues = new ArrayList<>(targetRows.size());
        for (Object[] row : targetRows) {
            Map<Integer, List<String>> rowMap = new HashMap<>();
            for (int colIdx : usedColumns) {
                List<String> values = colIdx < row.length
                        ? toValueList(row[colIdx])
                        : Collections.emptyList();
                rowMap.put(colIdx, values);
            }
            itemValues.add(rowMap);
        }
        return itemValues;
    }

    /**
     * Build all windows for all rules. Each window holds a reference to its rule.
     */
    private static List<WindowInfo> buildWindows(List<Rule> rules, int limit) {
        List<WindowInfo> windows = new ArrayList<>();
        for (Rule rule : rules) {
            if (!rule.isValidOp()) {
                continue;
            }
            for (int offset = 0; offset < rule.windowNum; offset++) {
                int start = rule.windowStart - 1 + offset;
                int end = Math.min(start + rule.windowSize, limit);
                if (start >= limit) {
                    break;
                }
                windows.add(new WindowInfo(start, end, rule));
            }
        }
        return windows;
    }

    /**
     * Build per-position window lists.
     */
    private static List<List<WindowInfo>> buildWindowsPerPosition(
            List<WindowInfo> windows,
            int limit) {
        List<List<WindowInfo>> windowsPerPosition = new ArrayList<>(limit);
        for (int position = 0; position < limit; position++) {
            List<WindowInfo> active = new ArrayList<>();
            for (WindowInfo window : windows) {
                if (window.covers(position)) {
                    active.add(window);
                }
            }
            windowsPerPosition.add(active);
        }
        return windowsPerPosition;
    }

    private static int[] greedyAssign(
            int itemCount,
            int limit,
            List<Map<Integer, List<String>>> itemValues,
            List<List<WindowInfo>> windowsPerPosition) {
        boolean[] assigned = new boolean[itemCount];
        int[] selection = new int[limit];

        for (int position = 0; position < limit; position++) {
            int bestCandidate = -1;
            double bestScore = Double.MAX_VALUE;
            List<WindowInfo> activeWindows = windowsPerPosition.get(position);

            for (int candidate = 0; candidate < itemCount; candidate++) {
                if (assigned[candidate]) {
                    continue;
                }
                if (bestCandidate == -1) {
                    bestCandidate = candidate;
                }

                double violation = computeViolation(activeWindows, itemValues.get(candidate));

                // Short-circuit: zero violation + iterating in order = best possible
                if (violation == 0) {
                    bestCandidate = candidate;
                    break;
                }

                double score = violation * VIOLATION_PENALTY + (candidate + 1);
                if (score < bestScore) {
                    bestScore = score;
                    bestCandidate = candidate;
                }
            }

            selection[position] = bestCandidate;
            assigned[bestCandidate] = true;
            updateWindowCounts(activeWindows, itemValues.get(bestCandidate));
        }
        return selection;
    }

    private static double computeViolation(List<WindowInfo> activeWindows,
                                           Map<Integer, List<String>> itemRow) {
        double violation = 0;
        for (WindowInfo win : activeWindows) {
            violation += win.simulateViolation(itemRow);
        }
        return violation;
    }

    private static void updateWindowCounts(List<WindowInfo> activeWindows,
                                           Map<Integer, List<String>> itemRow) {
        for (WindowInfo win : activeWindows) {
            win.add(itemRow);
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

    private static final class RuleFields {
        private final int windowSize;
        private final int windowStart;
        private final int windowNum;
        private final int diversityColumn;
        private final int diversityValue;
        private final int operator;
        private final int diversityNum;
        private final int weight;

        private RuleFields(List<RelDataTypeField> fields) {
            windowSize = requiredIndex(fields, COL_WINDOW_SIZE);
            windowStart = requiredIndex(fields, COL_WINDOW_START);
            windowNum = requiredIndex(fields, COL_WINDOW_NUM);
            diversityColumn = requiredIndex(fields, COL_DIVERSITY_COLUMN);
            diversityValue = requiredIndex(fields, COL_DIVERSITY_VALUE);
            operator = requiredIndex(fields, COL_OP);
            diversityNum = requiredIndex(fields, COL_DIVERSITY_NUM);
            weight = requiredIndex(fields, COL_WEIGHT);
        }

        private static RuleFields from(List<RelDataTypeField> fields) {
            return new RuleFields(fields);
        }

        private static int requiredIndex(List<RelDataTypeField> fields, String name) {
            int index = DataTypeUtils.findFieldIndex(fields, name);
            if (index < 0) {
                throw new IllegalArgumentException("rule table missing column: " + name);
            }
            return index;
        }

        private int lastIndex() {
            return Math.max(
                    Math.max(Math.max(windowSize, windowStart), Math.max(windowNum, diversityColumn)),
                    Math.max(Math.max(diversityValue, operator), Math.max(diversityNum, weight)));
        }
    }

    private static final class Rule {
        static final String OP_GT = ">";
        static final String OP_EQ = "=";
        static final String OP_LT = "<";

        final int windowSize;
        final int windowStart;
        final int windowNum;
        final String diversityValue;
        final String op;
        final int diversityNum;
        final double weight;
        final int columnIndex;
        final boolean appliesToEachValue;

        private Rule(
                int windowSize,
                int windowStart,
                int windowNum,
                String diversityValue,
                String op,
                int diversityNum,
                double weight,
                int columnIndex) {
            this.windowSize = windowSize;
            this.windowStart = windowStart;
            this.windowNum = windowNum;
            this.diversityValue = diversityValue;
            this.op = op;
            this.diversityNum = diversityNum;
            this.weight = weight;
            this.columnIndex = columnIndex;
            this.appliesToEachValue = diversityValue == null || diversityValue.isEmpty();
        }

        static Rule fromRow(
                Object[] row,
                RuleFields fields,
                List<RelDataTypeField> targetFields) {
            if (row.length <= fields.lastIndex()) {
                throw new IllegalArgumentException("Rule row has " + row.length
                        + " columns, expected at least " + (fields.lastIndex() + 1));
            }

            int windowSize = toInt(row[fields.windowSize]);
            int windowStart = toInt(row[fields.windowStart]);
            int windowNum = toInt(row[fields.windowNum]);
            String diversityColumn = RuleDiversity.toString(row[fields.diversityColumn]);
            String diversityValue = RuleDiversity.toString(row[fields.diversityValue]);
            String op = normalizeOp(RuleDiversity.toString(row[fields.operator]));
            int diversityNum = toInt(row[fields.diversityNum]);
            double weight = toDouble(row[fields.weight]);

            if (windowSize <= 0) {
                throw new IllegalArgumentException("window_size must be > 0, got " + windowSize);
            }
            if (windowStart < 1) {
                throw new IllegalArgumentException("window_start must be >= 1, got " + windowStart);
            }
            if (windowNum < 1) {
                throw new IllegalArgumentException("window_num must be >= 1, got " + windowNum);
            }
            if (diversityColumn == null || diversityColumn.isEmpty()) {
                throw new IllegalArgumentException("diversity_column must not be null or empty");
            }
            if (op == null) {
                throw new IllegalArgumentException("op must not be null");
            }
            if (diversityNum < 0) {
                throw new IllegalArgumentException("diversity_num must be >= 0, got " + diversityNum);
            }
            if (weight < 0) {
                throw new IllegalArgumentException("weight must be >= 0, got " + weight);
            }

            int columnIndex = DataTypeUtils.findFieldIndex(targetFields, diversityColumn);
            if (columnIndex < 0) {
                throw new IllegalArgumentException(
                        "diversity_column '" + diversityColumn + "' not found in target table");
            }
            return new Rule(windowSize, windowStart, windowNum, diversityValue, op,
                    diversityNum, weight, columnIndex);
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

    private static final class WindowInfo {
        final int start;
        final int end;
        final Rule rule;
        int matchedCount;
        final Map<String, Integer> valueCounts;

        WindowInfo(int start, int end, Rule rule) {
            this.start = start;
            this.end = end;
            this.rule = rule;
            this.valueCounts = rule.appliesToEachValue ? new HashMap<>() : null;
        }

        boolean covers(int position) {
            return position >= start && position < end;
        }

        /**
         * Simulate placing an item and compute the weighted violation penalty.
         */
        double simulateViolation(Map<Integer, List<String>> itemRow) {
            List<String> values = itemRow.getOrDefault(rule.columnIndex, Collections.emptyList());

            if (rule.appliesToEachValue) {
                if (values.isEmpty()) {
                    return 0;
                }
                Map<String, Integer> newCounts = new HashMap<>(valueCounts);
                for (String val : values) {
                    newCounts.merge(val, 1, Integer::sum);
                }
                double maxViol = 0;
                for (int count : newCounts.values()) {
                    double v = rule.calcViolation(count);
                    if (v > maxViol) {
                        maxViol = v;
                    }
                }
                return maxViol * rule.weight;
            }
            int newCount = matchedCount + (values.contains(rule.diversityValue) ? 1 : 0);
            return rule.calcViolation(newCount) * rule.weight;
        }

        /**
         * Update window counts after placing an item.
         */
        void add(Map<Integer, List<String>> itemRow) {
            List<String> values = itemRow.getOrDefault(rule.columnIndex, Collections.emptyList());

            if (rule.appliesToEachValue) {
                for (String val : values) {
                    valueCounts.merge(val, 1, Integer::sum);
                }
            } else if (values.contains(rule.diversityValue)) {
                matchedCount++;
            }
        }
    }
}
