package com.sqlrec.utils;

import com.sqlrec.runtime.BindableInterface;

import java.util.*;

public class TopologicalSortUtils {
    public static Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> topologicalSort(List<BindableInterface> bindableList) {
        Map<Integer, Set<Integer>> bindableDependency = buildBindableDependency(bindableList);
        bindableDependency = optimizeDependency(bindableDependency);
        List<Integer> sortedBindableList = topologicalSort(bindableDependency);

        return Map.entry(sortedBindableList, bindableDependency);
    }

    public static Map<Integer, Set<Integer>> optimizeDependency(Map<Integer, Set<Integer>> bindableDependency) {
        if (bindableDependency == null || bindableDependency.isEmpty()) {
            return bindableDependency;
        }

        Map<Integer, Set<Integer>> optimized = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
            optimized.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Map.Entry<Integer, Set<Integer>> entry : optimized.entrySet()) {
            Integer node = entry.getKey();
            Set<Integer> directDeps = entry.getValue();
            Set<Integer> toRemove = new HashSet<>();

            for (Integer dep : directDeps) {
                if (hasIndirectPath(optimized, node, dep, directDeps)) {
                    toRemove.add(dep);
                }
            }

            directDeps.removeAll(toRemove);
        }

        return optimized;
    }

    private static boolean hasIndirectPath(
            Map<Integer, Set<Integer>> dependency,
            Integer start,
            Integer target,
            Set<Integer> excludeDirectDeps
    ) {
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> queue = new LinkedList<>();

        Set<Integer> intermediateNodes = new HashSet<>(dependency.getOrDefault(start, Collections.emptySet()));
        intermediateNodes.remove(target);

        for (Integer intermediate : intermediateNodes) {
            if (!intermediate.equals(target)) {
                queue.offer(intermediate);
                visited.add(intermediate);
            }
        }

        while (!queue.isEmpty()) {
            Integer current = queue.poll();

            if (current.equals(target)) {
                return true;
            }

            Set<Integer> nextDeps = dependency.getOrDefault(current, Collections.emptySet());
            for (Integer next : nextDeps) {
                if (!visited.contains(next) && !next.equals(start)) {
                    if (next.equals(target)) {
                        return true;
                    }
                    visited.add(next);
                    queue.offer(next);
                }
            }
        }

        return false;
    }

    public static List<Integer> topologicalSort(Map<Integer, Set<Integer>> bindableDependency) {
        Map<Integer, Set<Integer>> forwardDependency = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
            forwardDependency.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        List<Integer> sortedBindableList = new ArrayList<>();
        while (!forwardDependency.isEmpty()) {
            List<Integer> noDependBindableList = new ArrayList<>();
            for (Map.Entry<Integer, Set<Integer>> entry : forwardDependency.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    noDependBindableList.add(entry.getKey());
                }
            }
            if (noDependBindableList.isEmpty()) {
                throw new RuntimeException("circular dependency");
            }
            sortedBindableList.addAll(noDependBindableList);
            for (Integer bindableIndex : noDependBindableList) {
                forwardDependency.remove(bindableIndex);
                for (Set<Integer> dependBindableSet : forwardDependency.values()) {
                    dependBindableSet.remove(bindableIndex);
                }
            }
        }

        return sortedBindableList;
    }

    public static Map<Integer, Set<Integer>> buildBindableDependency(List<BindableInterface> bindableList) {
        return buildBindableDependency(bindableList, false);
    }

    private static Map<Integer, Set<Integer>> buildBindableDependency(
            List<BindableInterface> bindableList,
            boolean dataDependenciesOnly
    ) {
        Map<String, Set<Integer>> readTableToBindableIndex = new HashMap<>();
        Map<String, Set<Integer>> writeTableToBindableIndex = new HashMap<>();
        Map<Integer, Set<Integer>> bindableDependency = new HashMap<>();

        for (int i = 0; i < bindableList.size(); i++) {
            bindableDependency.putIfAbsent(i, new HashSet<>());
        }

        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);

            boolean executionBarrier = !bindable.isParallelizable() || bindable.containsReturn();
            if (executionBarrier && !dataDependenciesOnly) {
                for (int j = 0; j < i; j++) {
                    bindableDependency.get(i).add(j);
                }
                for (int j = i + 1; j < bindableList.size(); j++) {
                    bindableDependency.get(j).add(i);
                }
                continue;
            }

            for (String readTable : bindable.getReadTables()) {
                readTable = NodeUtils.normalizeTableName(readTable);
                Set<Integer> writeBindableSet = writeTableToBindableIndex.computeIfAbsent(readTable, k -> new HashSet<>());
                for (Integer writeBindableIndex : writeBindableSet) {
                    bindableDependency.get(i).add(writeBindableIndex);
                }
            }

            for (String writeTable : bindable.getWriteTables()) {
                writeTable = NodeUtils.normalizeTableName(writeTable);
                Set<Integer> readBindableSet = readTableToBindableIndex.computeIfAbsent(writeTable, k -> new HashSet<>());
                Set<Integer> writeBindableSet = writeTableToBindableIndex.computeIfAbsent(writeTable, k -> new HashSet<>());

                Set<Integer> totalDependSet = new HashSet<>();
                totalDependSet.addAll(readBindableSet);
                totalDependSet.addAll(writeBindableSet);

                for (Integer dependBindableIndex : totalDependSet) {
                    bindableDependency.get(i).add(dependBindableIndex);
                }
            }

            for (String readTable : bindable.getReadTables()) {
                readTable = NodeUtils.normalizeTableName(readTable);
                if (!readTableToBindableIndex.containsKey(readTable)) {
                    readTableToBindableIndex.put(readTable, new HashSet<>());
                }
                readTableToBindableIndex.get(readTable).add(i);
            }
            for (String writeTable : bindable.getWriteTables()) {
                writeTable = NodeUtils.normalizeTableName(writeTable);
                if (!writeTableToBindableIndex.containsKey(writeTable)) {
                    writeTableToBindableIndex.put(writeTable, new HashSet<>());
                }
                writeTableToBindableIndex.get(writeTable).add(i);
            }
        }

        return bindableDependency;
    }

    /*
    set true if:
    1. the bindable is only union source, directly or indirectly
    2. the bindable is not source of result table, directly or indirectly
     */
    public static Map<Integer, Boolean> getIsUnionSource(
            List<BindableInterface> bindableList,
            List<Integer> sortedBindableList
    ) {
        Map<Integer, Boolean> isUnionSource = new HashMap<>();
        Map<Integer, Boolean> isUnionNode = new HashMap<>();

        // Execution barriers add control edges to the execution graph. Exception-ignore
        // analysis must use only real table dependencies, otherwise a SET or RETURN between
        // a source and its UNION consumer incorrectly disables branch degradation.
        Map<Integer, Set<Integer>> exceptionDependency = optimizeDependency(
                buildBindableDependency(bindableList, true)
        );
        Map<Integer, Set<Integer>> reverseBindableDependency = getReverseBindableDependency(exceptionDependency);

        for (int i = sortedBindableList.size() - 1; i >= 0; i--) {
            int bindableIndex = sortedBindableList.get(i);
            BindableInterface bindable = bindableList.get(bindableIndex);
            isUnionNode.put(bindableIndex, bindable.isUnionSql());

            // A RETURN node is a result sink. Keep its non-UNION inputs on the result path
            // from being treated as ignorable UNION-only sources.
            if (bindable.containsReturn()) {
                isUnionSource.put(bindableIndex, false);
                continue;
            }

            Set<Integer> consumers = reverseBindableDependency.getOrDefault(
                    bindableIndex, Collections.emptySet());
            boolean reachesUnion = false;
            boolean hasNonUnionConsumer = false;
            for (Integer consumerIndex : consumers) {
                if (isUnionNode.getOrDefault(consumerIndex, false)
                        || isUnionSource.getOrDefault(consumerIndex, false)) {
                    reachesUnion = true;
                } else {
                    hasNonUnionConsumer = true;
                    break;
                }
            }

            // A node is recoverable only when it actually feeds a UNION and every one of
            // its consumers stays on a UNION-only path. Unused non-UNION work must fail.
            isUnionSource.put(bindableIndex, reachesUnion && !hasNonUnionConsumer);
        }

        return isUnionSource;
    }

    public static Map<Integer, Set<Integer>> getReverseBindableDependency(
            Map<Integer, Set<Integer>> bindableDependency
    ) {
        Map<Integer, Set<Integer>> reverseBindableDependency = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
            Integer bindableIndex = entry.getKey();
            Set<Integer> dependBindableSet = entry.getValue();
            for (Integer dependBindableIndex : dependBindableSet) {
                if (!reverseBindableDependency.containsKey(dependBindableIndex)) {
                    reverseBindableDependency.put(dependBindableIndex, new HashSet<>());
                }
                reverseBindableDependency.get(dependBindableIndex).add(bindableIndex);
            }
        }
        return reverseBindableDependency;
    }
}
