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
            Map<Integer, Set<Integer>> reduced = TopologicalSortUtils.optimizeDependency(dependency);
            List<Integer> sorted = TopologicalSortUtils.topologicalSort(reduced);

            assertEquals(original, dependency);
            assertEquals(nodes.size(), sorted.size());
            assertEquals(dependency.keySet(), new HashSet<>(sorted));
            for (int node : nodes) {
                assertEquals(reachable(dependency, node), reachable(reduced, node));
                for (int predecessor : dependency.get(node)) {
                    assertTrue(sorted.indexOf(predecessor) < sorted.indexOf(node));
                }
                for (int predecessor : Set.copyOf(reduced.get(node))) {
                    reduced.get(node).remove(predecessor);
                    assertFalse(reachable(reduced, node).contains(predecessor),
                            "a reduced edge must not have an alternative path");
                    reduced.get(node).add(predecessor);
                }
            }
        }
    }

    private Set<Integer> reachable(Map<Integer, Set<Integer>> graph, int node) {
        Set<Integer> visited = new HashSet<>();
        Deque<Integer> remaining = new ArrayDeque<>(graph.get(node));
        while (!remaining.isEmpty()) {
            int current = remaining.removeFirst();
            if (visited.add(current)) {
                remaining.addAll(graph.get(current));
            }
        }
        return visited;
    }
}
