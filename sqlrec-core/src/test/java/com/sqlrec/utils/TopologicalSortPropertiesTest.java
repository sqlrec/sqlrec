package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TopologicalSortPropertiesTest {
    @Test
    void reductionPreservesReachabilityAndSortRespectsEveryOriginalEdge() {
        Random random = new Random(20261017);
        for (int trial = 0; trial < 100; trial++) {
            List<Integer> nodes = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                nodes.add(i * 17 - 100);
            }
            Collections.shuffle(nodes, random);
            Map<Integer, Set<Integer>> dependency = new HashMap<>();
            for (int i = 0; i < nodes.size(); i++) {
                Set<Integer> predecessors = new HashSet<>();
                for (int j = 0; j < i; j++) {
                    if (random.nextBoolean()) {
                        predecessors.add(nodes.get(j));
                    }
                }
                dependency.put(nodes.get(i), Set.copyOf(predecessors));
            }
            Map<Integer, Set<Integer>> original = Map.copyOf(dependency);
            TopologicalSortUtils.DependencyPlan plan = TopologicalSortUtils.planDependencies(dependency);
            Map<Integer, Set<Integer>> reduced = plan.dependencies();
            List<Integer> sorted = plan.executionOrder();

            assertEquals(original, dependency);
            assertEquals(nodes.size(), sorted.size());
            assertEquals(dependency.keySet(), new HashSet<>(sorted));
            for (int node : nodes) {
                assertEquals(reachableFrom(dependency, dependency.get(node)),
                        reachableFrom(reduced, reduced.get(node)));
                for (int predecessor : dependency.get(node)) {
                    assertTrue(sorted.indexOf(predecessor) < sorted.indexOf(node));
                }
                for (int predecessor : reduced.get(node)) {
                    Set<Integer> otherPredecessors = new HashSet<>(reduced.get(node));
                    otherPredecessors.remove(predecessor);
                    assertFalse(reachableFrom(reduced, otherPredecessors).contains(predecessor),
                            "a reduced edge must not have an alternative path");
                }
            }
        }
    }

    private Set<Integer> reachableFrom(Map<Integer, Set<Integer>> graph, Set<Integer> predecessors) {
        Set<Integer> visited = new HashSet<>();
        Deque<Integer> remaining = new ArrayDeque<>(predecessors);
        while (!remaining.isEmpty()) {
            int current = remaining.removeFirst();
            if (visited.add(current)) {
                remaining.addAll(graph.get(current));
            }
        }
        return visited;
    }
}
