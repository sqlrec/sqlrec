package com.sqlrec.utils;

import com.sqlrec.runtime.BindableInterface;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TopologicalSortUtilsTest {

    @Test
    public void testTopologicalSortWithEmptyMap() {
        Map<Integer, Set<Integer>> emptyDependency = new HashMap<>();
        List<Integer> result = TopologicalSortUtils.topologicalSort(emptyDependency);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testTopologicalSortWithSingleElement() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());

        List<Integer> result = TopologicalSortUtils.topologicalSort(dependency);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(0, result.get(0));
    }

    @Test
    public void testTopologicalSortWithLinearDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(1)));

        List<Integer> result = TopologicalSortUtils.topologicalSort(dependency);
        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.indexOf(0) < result.indexOf(1));
        assertTrue(result.indexOf(1) < result.indexOf(2));
    }

    @Test
    public void testTopologicalSortWithMultipleDependencies() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>());
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));

        List<Integer> result = TopologicalSortUtils.topologicalSort(dependency);
        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.indexOf(0) < result.indexOf(2));
        assertTrue(result.indexOf(1) < result.indexOf(2));
    }

    @Test
    public void testTopologicalSortWithCircularDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>(Collections.singleton(1)));
        dependency.put(1, new HashSet<>(Collections.singleton(0)));

        assertThrows(RuntimeException.class, () -> {
            TopologicalSortUtils.topologicalSort(dependency);
        });
    }

    @Test
    public void testGetReverseBindableDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));

        Map<Integer, Set<Integer>> reverse = TopologicalSortUtils.getReverseBindableDependency(dependency);

        assertNotNull(reverse);
        assertEquals(2, reverse.size());
        assertTrue(reverse.containsKey(0));
        assertTrue(reverse.containsKey(1));
        assertEquals(2, reverse.get(0).size());
        assertTrue(reverse.get(0).contains(1));
        assertTrue(reverse.get(0).contains(2));
        assertEquals(1, reverse.get(1).size());
        assertTrue(reverse.get(1).contains(2));
    }

    @Test
    public void testBuildBindableDependencyWithNoDependencies() {
        List<BindableInterface> bindableList = new ArrayList<>();
        bindableList.add(createTestBindable(true, Set.of("t1"), Set.of("t2")));
        bindableList.add(createTestBindable(true, Set.of("t3"), Set.of("t4")));

        Map<Integer, Set<Integer>> dependency = TopologicalSortUtils.buildBindableDependency(bindableList);

        assertNotNull(dependency);
        assertEquals(2, dependency.size());
        assertTrue(dependency.get(0).isEmpty());
        assertTrue(dependency.get(1).isEmpty());
    }

    @Test
    public void testBuildBindableDependencyWithReadAfterWrite() {
        List<BindableInterface> bindableList = new ArrayList<>();
        bindableList.add(createTestBindable(true, Set.of(), Set.of("t1")));
        bindableList.add(createTestBindable(true, Set.of("t1"), Set.of()));

        Map<Integer, Set<Integer>> dependency = TopologicalSortUtils.buildBindableDependency(bindableList);

        assertNotNull(dependency);
        assertEquals(2, dependency.size());
        assertTrue(dependency.get(0).isEmpty());
        assertEquals(1, dependency.get(1).size());
        assertTrue(dependency.get(1).contains(0));
    }

    @Test
    public void testBuildBindableDependencyWithWriteAfterWrite() {
        List<BindableInterface> bindableList = new ArrayList<>();
        bindableList.add(createTestBindable(true, Set.of(), Set.of("t1")));
        bindableList.add(createTestBindable(true, Set.of(), Set.of("t1")));

        Map<Integer, Set<Integer>> dependency = TopologicalSortUtils.buildBindableDependency(bindableList);

        assertNotNull(dependency);
        assertEquals(2, dependency.size());
        assertTrue(dependency.get(0).isEmpty());
        assertEquals(1, dependency.get(1).size());
        assertTrue(dependency.get(1).contains(0));
    }

    @Test
    public void testBuildBindableDependencyWithNonParallelizable() {
        List<BindableInterface> bindableList = new ArrayList<>();
        bindableList.add(createTestBindable(true, Set.of("t1"), Set.of("t2")));
        bindableList.add(createTestBindable(false, Set.of("t3"), Set.of("t4")));
        bindableList.add(createTestBindable(true, Set.of("t5"), Set.of("t6")));

        Map<Integer, Set<Integer>> dependency = TopologicalSortUtils.buildBindableDependency(bindableList);

        assertNotNull(dependency);
        assertEquals(3, dependency.size());
        assertTrue(dependency.get(0).isEmpty());
        assertEquals(1, dependency.get(1).size());
        assertTrue(dependency.get(1).contains(0));
        assertEquals(1, dependency.get(2).size());
        assertTrue(dependency.get(2).contains(1));
    }

    @Test
    public void testTopologicalSortWithBindableList() {
        List<BindableInterface> bindableList = new ArrayList<>();
        bindableList.add(createTestBindable(true, Set.of(), Set.of("t1")));
        bindableList.add(createTestBindable(true, Set.of("t1"), Set.of("t2")));
        bindableList.add(createTestBindable(true, Set.of("t2"), Set.of()));

        Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> result =
                TopologicalSortUtils.topologicalSort(bindableList);

        assertNotNull(result);
        assertNotNull(result.getKey());
        assertNotNull(result.getValue());
        assertEquals(3, result.getKey().size());
        assertTrue(result.getKey().indexOf(0) < result.getKey().indexOf(1));
        assertTrue(result.getKey().indexOf(1) < result.getKey().indexOf(2));
    }

    @Test
    public void testOptimizeDependencyWithEmptyMap() {
        Map<Integer, Set<Integer>> emptyDependency = new HashMap<>();
        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(emptyDependency);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testOptimizeDependencyWithNull() {
        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(null);
        assertNull(result);
    }

    @Test
    public void testOptimizeDependencyWithSingleNode() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0).isEmpty());
    }

    @Test
    public void testOptimizeDependencySimpleChain() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(1));
        assertFalse(result.get(2).contains(0));
    }

    @Test
    public void testOptimizeDependencyNoRedundancy() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>());
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertEquals(2, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertTrue(result.get(2).contains(1));
    }

    @Test
    public void testOptimizeDependencyDiamond() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 1, 2)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(4, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(2, result.get(3).size());
        assertTrue(result.get(3).contains(1));
        assertTrue(result.get(3).contains(2));
        assertFalse(result.get(3).contains(0));
    }

    @Test
    public void testOptimizeDependencyLongChain() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 1, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 1, 2, 3)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(1));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(2));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
    }

    @Test
    public void testOptimizeDependencyMultiplePaths() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Arrays.asList(1, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 1, 2, 3)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(2, result.get(3).size());
        assertTrue(result.get(3).contains(1));
        assertTrue(result.get(3).contains(2));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
    }

    @Test
    public void testOptimizeDependencyComplexGraph() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Arrays.asList(1, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 3)));
        dependency.put(5, new HashSet<>(Arrays.asList(0, 1, 2, 3, 4)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(6, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(2, result.get(3).size());
        assertTrue(result.get(3).contains(1));
        assertTrue(result.get(3).contains(2));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
        assertEquals(1, result.get(5).size());
        assertTrue(result.get(5).contains(4));
    }

    @Test
    public void testOptimizeDependencyPreservesOriginal() {
        Map<Integer, Set<Integer>> original = new HashMap<>();
        original.put(0, new HashSet<>());
        original.put(1, new HashSet<>(Collections.singleton(0)));
        original.put(2, new HashSet<>(Arrays.asList(0, 1)));

        Map<Integer, Set<Integer>> copy = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : original.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        TopologicalSortUtils.optimizeDependency(original);

        assertEquals(2, original.get(2).size());
        assertTrue(original.get(2).contains(0));
        assertTrue(original.get(2).contains(1));
    }

    @Test
    public void testOptimizeDependencyWithSelfDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>(Collections.singleton(0)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0).contains(0));
    }

    @Test
    public void testOptimizeDependencyWithIsolatedNodes() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>());
        dependency.put(2, new HashSet<>());

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());
    }

    @Test
    public void testOptimizeDependencyWithPartialRedundancy() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 1)));
        dependency.put(4, new HashSet<>(Arrays.asList(1, 2)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(1));
        assertFalse(result.get(3).contains(0));
        assertEquals(2, result.get(4).size());
        assertTrue(result.get(4).contains(1));
        assertTrue(result.get(4).contains(2));
    }

    @Test
    public void testOptimizeDependencyDeepChain() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        for (int i = 1; i <= 10; i++) {
            Set<Integer> deps = new HashSet<>();
            for (int j = 0; j < i; j++) {
                deps.add(j);
            }
            dependency.put(i, deps);
        }

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(11, result.size());
        assertTrue(result.get(0).isEmpty());
        for (int i = 1; i <= 10; i++) {
            assertEquals(1, result.get(i).size());
            assertTrue(result.get(i).contains(i - 1));
        }
    }

    @Test
    public void testOptimizeDependencyWithTransitiveReduction() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 1, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 1, 2, 3)));
        dependency.put(5, new HashSet<>(Arrays.asList(0, 2, 4)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(6, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(1));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(2));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
        assertEquals(1, result.get(5).size());
        assertTrue(result.get(5).contains(4));
    }

    @Test
    public void testOptimizeDependencyMultipleSources() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>());
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 1, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 1, 2, 3)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertEquals(2, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertTrue(result.get(2).contains(1));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(2));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
    }

    @Test
    public void testOptimizeDependencyWithThreeWayBranch() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Collections.singleton(0)));
        dependency.put(4, new HashSet<>(Arrays.asList(1, 2, 3)));
        dependency.put(5, new HashSet<>(Arrays.asList(0, 1, 2, 3, 4)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(6, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(0));
        assertEquals(3, result.get(4).size());
        assertTrue(result.get(4).contains(1));
        assertTrue(result.get(4).contains(2));
        assertTrue(result.get(4).contains(3));
        assertEquals(1, result.get(5).size());
        assertTrue(result.get(5).contains(4));
    }

    @Test
    public void testOptimizeDependencyWithMixedDepths() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(0)));
        dependency.put(3, new HashSet<>(Collections.singleton(1)));
        dependency.put(4, new HashSet<>(Collections.singleton(2)));
        dependency.put(5, new HashSet<>(Arrays.asList(0, 1, 2, 3, 4)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(6, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(0));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(1));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(2));
        assertEquals(2, result.get(5).size());
        assertTrue(result.get(5).contains(3));
        assertTrue(result.get(5).contains(4));
    }

    @Test
    public void testOptimizeDependencyWithSkipLevelDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Collections.singleton(1)));
        dependency.put(3, new HashSet<>(Arrays.asList(0, 2)));
        dependency.put(4, new HashSet<>(Arrays.asList(0, 1, 2, 3)));

        Map<Integer, Set<Integer>> result = TopologicalSortUtils.optimizeDependency(dependency);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.get(0).isEmpty());
        assertEquals(1, result.get(1).size());
        assertTrue(result.get(1).contains(0));
        assertEquals(1, result.get(2).size());
        assertTrue(result.get(2).contains(1));
        assertEquals(1, result.get(3).size());
        assertTrue(result.get(3).contains(2));
        assertFalse(result.get(3).contains(0));
        assertEquals(1, result.get(4).size());
        assertTrue(result.get(4).contains(3));
    }

    @Test
    public void testOptimizeDependencyIntegrationWithTopologicalSort() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Collections.singleton(0)));
        dependency.put(2, new HashSet<>(Arrays.asList(0, 1)));

        Map<Integer, Set<Integer>> optimized = TopologicalSortUtils.optimizeDependency(dependency);
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(optimized);

        assertNotNull(sorted);
        assertEquals(3, sorted.size());
        assertTrue(sorted.indexOf(0) < sorted.indexOf(1));
        assertTrue(sorted.indexOf(1) < sorted.indexOf(2));
    }

    private BindableInterface createTestBindable(boolean parallelizable, Set<String> readTables, Set<String> writeTables) {
        return new BindableInterface() {
            @Override
            public Enumerable<Object[]> bind(CalciteSchema schema, com.sqlrec.common.runtime.ExecuteContext context) {
                return null;
            }

            @Override
            public List<RelDataTypeField> getReturnDataFields() {
                return null;
            }

            @Override
            public boolean isParallelizable() {
                return parallelizable;
            }

            @Override
            public Set<String> getReadTables() {
                return readTables;
            }

            @Override
            public Set<String> getWriteTables() {
                return writeTables;
            }
        };
    }
}
