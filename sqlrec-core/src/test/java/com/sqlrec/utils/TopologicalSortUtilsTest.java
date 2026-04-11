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
