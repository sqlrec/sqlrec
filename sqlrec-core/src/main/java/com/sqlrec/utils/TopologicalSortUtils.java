package com.sqlrec.utils;

import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.CalciteBindable;

import java.util.*;

public class TopologicalSortUtils {
    public static Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> topologicalSort(List<BindableInterface> bindableList) {
        Map<Integer, Set<Integer>> bindableDependency = buildBindableDependency(bindableList);
        List<Integer> sortedBindableList = topologicalSort(bindableDependency);

        return Map.entry(sortedBindableList, bindableDependency);
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
        Map<String, Set<Integer>> readTableToBindableIndex = new HashMap<>();
        Map<String, Set<Integer>> writeTableToBindableIndex = new HashMap<>();
        Map<Integer, Set<Integer>> bindableDependency = new HashMap<>();

        for (int i = 0; i < bindableList.size(); i++) {
            bindableDependency.putIfAbsent(i, new HashSet<>());
        }

        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);

            if (!bindable.isParallelizable()) {
                for (int j = 0; j < i; j++) {
                    bindableDependency.get(i).add(j);
                }
                for (int j = i + 1; j < bindableList.size(); j++) {
                    bindableDependency.get(j).add(i);
                }
                continue;
            }

            for (String readTable : bindable.getReadTables()) {
                Set<Integer> writeBindableSet = writeTableToBindableIndex.computeIfAbsent(readTable, k -> new HashSet<>());
                for (Integer writeBindableIndex : writeBindableSet) {
                    bindableDependency.get(i).add(writeBindableIndex);
                }
            }

            for (String writeTable : bindable.getWriteTables()) {
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
                if (!readTableToBindableIndex.containsKey(readTable)) {
                    readTableToBindableIndex.put(readTable, new HashSet<>());
                }
                readTableToBindableIndex.get(readTable).add(i);
            }
            for (String writeTable : bindable.getWriteTables()) {
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
            Map<Integer, Set<Integer>> bindableDependency,
            List<Integer> sortedBindableList,
            String returnTableName
    ) {
        Map<Integer, Boolean> isUnionSource = new HashMap<>();
        Map<Integer, Boolean> isUnionNode = new HashMap<>();

        Map<Integer, Set<Integer>> reverseBindableDependency = getReverseBindableDependency(bindableDependency);

        for (int i = sortedBindableList.size() - 1; i >= 0; i--) {
            int bindableIndex = sortedBindableList.get(i);
            BindableInterface bindable = bindableList.get(bindableIndex);
            isUnionNode.put(bindableIndex, bindable.isUnionSql());

            if (reverseBindableDependency.containsKey(bindableIndex)) {
                Set<Integer> dependBindableSet = reverseBindableDependency.get(bindableIndex);
                for (Integer dependBindableIndex : dependBindableSet) {
                    if (!isUnionNode.getOrDefault(dependBindableIndex, false) &&
                            !isUnionSource.getOrDefault(dependBindableIndex, false)) {
                        isUnionSource.put(bindableIndex, false);
                        break;
                    }
                }
            }
            if (isUnionSource.containsKey(bindableIndex)) {
                continue;
            }

            if (bindable instanceof CacheTableBindable) {
                String cacheTableName = ((CacheTableBindable) bindable).getTableName();
                if (cacheTableName.equals(returnTableName)) {
                    isUnionSource.put(bindableIndex, false);
                    continue;
                }
            }

            isUnionSource.put(bindableIndex, true);
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
