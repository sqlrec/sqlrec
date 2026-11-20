package com.sqlrec.utils;

import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.IfBindable;
import com.sqlrec.runtime.ProxyAllBindable;

import java.util.*;

public class TopologicalSortUtils {
    public record DependencyPlan(
            List<Integer> executionOrder,
            Map<Integer, Set<Integer>> dependencies
    ) {
        public DependencyPlan {
            executionOrder = List.copyOf(executionOrder);
            Map<Integer, Set<Integer>> snapshot = new HashMap<>();
            dependencies.forEach((node, parents) -> snapshot.put(node, Set.copyOf(parents)));
            dependencies = Map.copyOf(snapshot);
        }
    }

    public static DependencyPlan topologicalSort(List<BindableInterface> bindableList) {
        return planDependencies(buildBindableDependency(bindableList));
    }

    static DependencyPlan planDependencies(Map<Integer, Set<Integer>> dependencies) {
        List<Integer> executionOrder = topologicalSort(dependencies);
        return new DependencyPlan(executionOrder, removeRedundantEdges(dependencies));
    }

    private static Map<Integer, Set<Integer>> removeRedundantEdges(Map<Integer, Set<Integer>> bindableDependency) {
        Map<Integer, Set<Integer>> optimized = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
            optimized.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Set<Integer> directDeps : optimized.values()) {
            Set<Integer> toRemove = new HashSet<>();

            for (Integer dep : directDeps) {
                if (hasIndirectPath(optimized, directDeps, dep)) {
                    toRemove.add(dep);
                }
            }

            directDeps.removeAll(toRemove);
        }

        return optimized;
    }

    private static boolean hasIndirectPath(
            Map<Integer, Set<Integer>> dependency,
            Set<Integer> directDeps,
            Integer target
    ) {
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> queue = new ArrayDeque<>();

        for (Integer dep : directDeps) {
            if (!dep.equals(target)) {
                visited.add(dep);
                queue.add(dep);
            }
        }

        while (!queue.isEmpty()) {
            Integer current = queue.remove();

            if (current.equals(target)) {
                return true;
            }

            for (Integer next : dependency.get(current)) {
                if (visited.add(next)) {
                    queue.add(next);
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

            Set<String> readTables = normalizeTableNames(bindable.getReadTables());
            Set<String> writeTables = normalizeTableNames(bindable.getWriteTables());
            Set<Integer> dependencies = bindableDependency.get(i);
            for (String readTable : readTables) {
                dependencies.addAll(writeTableToBindableIndex.getOrDefault(readTable, Collections.emptySet()));
            }

            for (String writeTable : writeTables) {
                dependencies.addAll(readTableToBindableIndex.getOrDefault(writeTable, Collections.emptySet()));
                dependencies.addAll(writeTableToBindableIndex.getOrDefault(writeTable, Collections.emptySet()));
            }

            for (String readTable : readTables) {
                readTableToBindableIndex.computeIfAbsent(readTable, k -> new HashSet<>()).add(i);
            }
            for (String writeTable : writeTables) {
                writeTableToBindableIndex.computeIfAbsent(writeTable, k -> new HashSet<>()).add(i);
            }
        }

        return bindableDependency;
    }

    private static Set<String> normalizeTableNames(Set<String> tableNames) {
        Set<String> normalized = new HashSet<>();
        for (String name : tableNames) {
            normalized.add(NodeUtils.normalizeTableName(name));
        }
        return normalized;
    }

    /**
     * Uses direct read edges, never execution barriers or transitive reduction.
     * Local conditional nodes and all writers of a repeatedly written table must
     * not recover automatically, nor may their upstream producers in this function.
     * Other branches and the internals of called functions keep their own policy.
     */
    public static Set<Integer> findRecoverableUnionSources(List<BindableInterface> bindableList) {
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
        // Every producer precedes its consumers, so both passes can use reverse statement order.
        for (int i = bindableList.size() - 1; i >= 0; i--) {
            if (protectedNodes.contains(i)) {
                protectedNodes.addAll(readDependencies.get(i));
            }
        }

        Map<Integer, Set<Integer>> tableConsumers = getReverseBindableDependency(readDependencies);
        Set<Integer> unionSources = new HashSet<>();
        for (int i = bindableList.size() - 1; i >= 0; i--) {
            if (protectedNodes.contains(i) || bindableList.get(i).containsReturn()) {
                continue;
            }
            Set<Integer> consumers = tableConsumers.getOrDefault(i, Collections.emptySet());
            if (!consumers.isEmpty() && consumers.stream().allMatch(consumer ->
                    bindableList.get(consumer).isUnionSql() || unionSources.contains(consumer))) {
                unionSources.add(i);
            }
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
                reverseBindableDependency.computeIfAbsent(dependBindableIndex, k -> new HashSet<>())
                        .add(bindableIndex);
            }
        }
        return reverseBindableDependency;
    }
}
