package com.sqlrec.utils;

import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.IfBindable;
import com.sqlrec.runtime.ProxyAllBindable;

import java.util.*;

public class TopologicalSortUtils {
    public static Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> topologicalSort(List<BindableInterface> bindableList) {
        Map<Integer, Set<Integer>> bindableDependency = buildBindableDependency(bindableList);
        bindableDependency = optimizeDependency(bindableDependency);
        List<Integer> sortedBindableList = topologicalSort(bindableDependency);

        return Map.entry(sortedBindableList, bindableDependency);
    }

    /** Removes redundant execution edges from a DAG without modifying the input. */
    public static Map<Integer, Set<Integer>> optimizeDependency(Map<Integer, Set<Integer>> bindableDependency) {
        if (bindableDependency == null || bindableDependency.isEmpty()) {
            return bindableDependency;
        }
        // Transitive reduction is only valid here for an acyclic, complete graph.
        topologicalSort(bindableDependency);

        Map<Integer, Set<Integer>> optimized = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
            optimized.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Map.Entry<Integer, Set<Integer>> entry : optimized.entrySet()) {
            Integer node = entry.getKey();
            Set<Integer> directDeps = entry.getValue();
            Set<Integer> toRemove = new HashSet<>();

            for (Integer dep : directDeps) {
                if (hasIndirectPath(optimized, node, dep)) {
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
            Integer target
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
            for (Integer dependency : entry.getValue()) {
                if (!bindableDependency.containsKey(dependency)) {
                    throw new IllegalArgumentException("unknown dependency node " + dependency
                            + " referenced by node " + entry.getKey());
                }
            }
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
                throw new IllegalArgumentException("circular dependency: " + forwardDependency.keySet());
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
        Map<String, Set<Integer>> readTableToBindableIndex = new HashMap<>();
        Map<String, Set<Integer>> writeTableToBindableIndex = new HashMap<>();
        Map<Integer, Set<Integer>> bindableDependency = new HashMap<>();

        for (int i = 0; i < bindableList.size(); i++) {
            bindableDependency.putIfAbsent(i, new HashSet<>());
        }

        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);

            boolean executionBarrier = !bindable.isParallelizable() || bindable.containsReturn();
            if (executionBarrier) {
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

    /**
     * Uses direct read edges, never execution barriers or transitive reduction.
     * Local conditional nodes and all writers of a repeatedly written table must
     * not recover automatically, nor may their upstream producers in this function.
     * Other branches and the internals of called functions keep their own policy.
     */
    public static Map<Integer, Boolean> getIsUnionSource(
            List<BindableInterface> bindableList,
            List<Integer> sortedBindableList
    ) {
        Map<String, Set<Integer>> tableWriters = new HashMap<>();
        Map<Integer, Set<Integer>> readDependencies = new HashMap<>();
        Set<Integer> protectedNodes = new HashSet<>();
        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);
            if (isIfNode(bindable)) {
                protectedNodes.add(i);
            }
            Set<Integer> producers = new HashSet<>();
            for (String tableName : bindable.getReadTables()) {
                producers.addAll(tableWriters.getOrDefault(
                        NodeUtils.normalizeTableName(tableName), Collections.emptySet()));
            }
            readDependencies.put(i, producers);
            // Register writes after reads to avoid a self-dependency.
            for (String tableName : bindable.getWriteTables()) {
                tableWriters.computeIfAbsent(NodeUtils.normalizeTableName(tableName), k -> new HashSet<>()).add(i);
            }
        }
        for (Set<Integer> writers : tableWriters.values()) {
            if (writers.size() > 1) {
                protectedNodes.addAll(writers);
            }
        }
        // Read edges always point to earlier statements, so one reverse pass suffices.
        for (int i = bindableList.size() - 1; i >= 0; i--) {
            if (protectedNodes.contains(i)) {
                protectedNodes.addAll(readDependencies.get(i));
            }
        }

        Map<Integer, Set<Integer>> tableConsumers = getReverseBindableDependency(readDependencies);
        Map<Integer, Boolean> unionSources = new HashMap<>();
        for (int i = sortedBindableList.size() - 1; i >= 0; i--) {
            int index = sortedBindableList.get(i);
            if (protectedNodes.contains(index) || bindableList.get(index).containsReturn()) {
                unionSources.put(index, false);
                continue;
            }
            Set<Integer> consumers = tableConsumers.getOrDefault(index, Collections.emptySet());
            boolean onlyUnionConsumers = !consumers.isEmpty() && consumers.stream().allMatch(consumer ->
                    bindableList.get(consumer).isUnionSql() || unionSources.getOrDefault(consumer, false));
            unionSources.put(index, onlyUnionConsumers);
        }
        return unionSources;
    }

    private static boolean isIfNode(BindableInterface bindable) {
        // Unwrap execution proxies only; a CALL is a separate function boundary.
        while (bindable instanceof ProxyAllBindable proxy) {
            bindable = proxy.getDelegate();
        }
        return bindable instanceof IfBindable;
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
