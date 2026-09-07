package com.sqlrec.utils;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.ConcurrentCalciteSchema;
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
    public void testTopologicalSortDoesNotMutateDependency() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, new HashSet<>());
        dependency.put(1, new HashSet<>(Set.of(0)));

        TopologicalSortUtils.topologicalSort(dependency);

        assertEquals(Set.of(), dependency.get(0));
        assertEquals(Set.of(0), dependency.get(1));
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
    public void testGetReverseBindableDependencyWithNoEdges() {
        Map<Integer, Set<Integer>> dependency = new HashMap<>();
        dependency.put(0, Set.of());
        dependency.put(1, Set.of());

        assertTrue(TopologicalSortUtils.getReverseBindableDependency(dependency).isEmpty());
    }

    @Test
    public void testTopologicalSortWithMinimalSqlFunction() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION minimal_function",
                "RETURN"
        ));

        assertEquals(List.of(0), TopologicalSortUtils.topologicalSort(function.getBindableList()).getKey());
        assertEquals(Set.of(), function.getBindableDependency().get(0));
    }

    @Test
    public void testBuildBindableDependencyWithNoDependencies() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION independent_writes",
                "CACHE TABLE t1 AS SELECT 1 AS id",
                "CACHE TABLE t2 AS SELECT 2 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertTrue(dependency.get(0).isEmpty());
        assertTrue(dependency.get(1).isEmpty());
    }

    @Test
    public void testBuildBindableDependencyWithReadAfterWrite() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION read_after_write",
                "CACHE TABLE t1 AS SELECT 1 AS id",
                "SELECT * FROM t1",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertTrue(dependency.get(0).isEmpty());
        assertEquals(Set.of(0), dependency.get(1));
    }

    @Test
    public void testBuildBindableDependencyAllowsConcurrentReads() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION concurrent_reads",
                "CACHE TABLE shared_table AS SELECT 1 AS id",
                "SELECT * FROM shared_table",
                "SELECT * FROM shared_table",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(), dependency.get(0));
        assertEquals(Set.of(0), dependency.get(1));
        assertEquals(Set.of(0), dependency.get(2));
        assertFalse(dependency.get(2).contains(1));
    }

    @Test
    public void testBuildBindableDependencyWithWriteAfterRead() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION write_after_read",
                "DEFINE INPUT TABLE shared_table(id INT)",
                "SELECT * FROM shared_table",
                "CACHE TABLE shared_table AS SELECT 1 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(0), dependency.get(1));
    }

    @Test
    public void testBuildBindableDependencyDoesNotCreateSelfDependency() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION read_and_write_same_table",
                "DEFINE INPUT TABLE shared_table(id INT)",
                "CACHE TABLE shared_table AS SELECT * FROM shared_table",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(), dependency.get(0));
    }

    @Test
    public void testBuildBindableDependencyCollectsAllPriorReadersAndWriters() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION all_prior_accesses",
                "DEFINE INPUT TABLE Shared_Table(id INT)",
                "SELECT * FROM Shared_Table",
                "CACHE TABLE shared_table AS SELECT 1 AS id",
                "SELECT * FROM SHARED_TABLE",
                "CACHE TABLE Shared_Table AS SELECT 2 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(), dependency.get(0));
        assertEquals(Set.of(0), dependency.get(1));
        assertEquals(Set.of(1), dependency.get(2));
        assertEquals(Set.of(0, 1, 2), dependency.get(3));
    }

    @Test
    public void testBuildBindableDependencyIsCaseInsensitive() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION case_insensitive_dependency",
                "CACHE TABLE Source_Table AS SELECT 1 AS id",
                "SELECT * FROM source_table",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(0), dependency.get(1));
    }

    @Test
    public void testCompiledFunctionTracksAliasedCacheTableDependency() throws Exception {
        SqlFunctionBindable function = compileFunction(
                List.of(
                        "CREATE SQL FUNCTION alias_dependency",
                        "CACHE TABLE Source_Table AS SELECT 1 AS id",
                        "CACHE TABLE result_cache AS SELECT * FROM source_table AS source_alias",
                        "RETURN result_cache"
                )
        );

        assertEquals(Set.of(0), function.getBindableDependency().get(1));
        assertTrue(function.getBindableDependency().get(2).contains(1));
        assertFalse(function.getAccessTables().contains("source_table"));
    }

    @Test
    public void testCompiledFunctionTracksCacheTableThroughAliasedSubquery() throws Exception {
        SqlFunctionBindable function = compileFunction(
                List.of(
                        "CREATE SQL FUNCTION subquery_dependency",
                        "CACHE TABLE source_table AS SELECT 1 AS id",
                        "CACHE TABLE result_cache AS SELECT * FROM (SELECT * FROM source_table) AS nested_source",
                        "RETURN result_cache"
                )
        );

        assertEquals(Set.of(0), function.getBindableDependency().get(1));
    }

    @Test
    public void testDynamicAsyncFunctionTracksInputTableDependency() throws Exception {
        SqlFunctionBindable function = compileFunction(
                List.of(
                        "CREATE SQL FUNCTION dynamic_async_dependency",
                        "CACHE TABLE Source_Table AS SELECT 1 AS id",
                        "CALL GET('function_name')(source_table) LIKE source_table ASYNC",
                        "RETURN source_table"
                )
        );

        assertEquals(Set.of("source_table"), function.getBindableList().get(1).getReadTables());
        assertEquals(Set.of(0), function.getBindableDependency().get(1));
    }

    private SqlFunctionBindable compileFunction(List<String> sql) throws Exception {
        FunctionCompiler compiler = new FunctionCompiler(
                ConcurrentCalciteSchema.createRootSchema(),
                new CompileManager()
        );
        compiler.compileAllSql(sql);
        return compiler.getFunctionBindable();
    }

    @Test
    public void testBuildBindableDependencyWithWriteAfterWrite() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION write_after_write",
                "CACHE TABLE t1 AS SELECT 1 AS id",
                "CACHE TABLE t1 AS SELECT 2 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertTrue(dependency.get(0).isEmpty());
        assertEquals(Set.of(0), dependency.get(1));
    }

    @Test
    public void testBuildBindableDependencyWithNonParallelizable() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION set_barrier",
                "CACHE TABLE t1 AS SELECT 1 AS id",
                "SET barrier_key=barrier_value",
                "CACHE TABLE t2 AS SELECT 2 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertTrue(dependency.get(0).isEmpty());
        assertEquals(Set.of(0), dependency.get(1));
        assertEquals(Set.of(1), dependency.get(2));
    }

    @Test
    public void testReturnNodeIsAnExecutionBarrier() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION return_barrier",
                "CACHE TABLE left_table AS SELECT 1 AS id",
                "CACHE TABLE right_table AS SELECT 2 AS id",
                "RETURN"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(0, 1), dependency.get(2));
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(
                TopologicalSortUtils.optimizeDependency(dependency)
        );
        assertTrue(sorted.indexOf(0) < sorted.indexOf(2));
        assertTrue(sorted.indexOf(1) < sorted.indexOf(2));
    }

    @Test
    public void testProxyWrappedNestedReturnStillActsAsExecutionBarrier() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION nested_return_barrier",
                "CACHE TABLE before_table AS SELECT 1 AS id",
                "IF (SELECT TRUE) THEN (RETURN SELECT 1 AS id)",
                "CACHE TABLE after_table AS SELECT 2 AS id",
                "RETURN after_table"
        ));
        Map<Integer, Set<Integer>> dependency =
                TopologicalSortUtils.buildBindableDependency(function.getBindableList());

        assertEquals(Set.of(0), dependency.get(1));
        assertTrue(dependency.get(2).contains(1));
    }

    @Test
    public void testExceptionAnalysisUsesReturnDataDependenciesOnly() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION ignored_return_edges",
                "CACHE TABLE unused_table AS SELECT 1 AS id",
                "CACHE TABLE result_table AS SELECT 2 AS id",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertFalse(unionSources.get(0), "unused non-UNION nodes must not hide failures");
        assertFalse(unionSources.get(1), "the actual result producer must not hide failures");
        assertFalse(unionSources.get(2), "RETURN is the result sink");
    }

    @Test
    public void testExceptionAnalysisAllowsSourcesFeedingReturnedUnion() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION returned_union",
                "CACHE TABLE left_table AS SELECT 1 AS id",
                "CACHE TABLE right_table AS SELECT 2 AS id",
                "CACHE TABLE result_table AS " +
                        "SELECT * FROM left_table UNION ALL SELECT * FROM right_table",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertTrue(unionSources.get(0));
        assertTrue(unionSources.get(1));
        assertFalse(unionSources.get(2));
        assertFalse(unionSources.get(3));
    }

    @Test
    public void testExceptionAnalysisTreatsEmptyReturnAsHavingNoDataProducer() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION empty_return",
                "CACHE TABLE unused_table AS SELECT 1 AS id",
                "RETURN"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertFalse(unionSources.get(0));
        assertFalse(unionSources.get(1));
    }

    @Test
    public void testExceptionAnalysisDoesNotIgnoreUnusedNonUnionChain() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION unused_chain",
                "CACHE TABLE unused_source AS SELECT 1 AS id",
                "CACHE TABLE unused_result AS SELECT * FROM unused_source",
                "RETURN"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertFalse(unionSources.get(0));
        assertFalse(unionSources.get(1));
        assertFalse(unionSources.get(2));
    }

    @Test
    public void testExceptionAnalysisRecognizesOrderedUnion() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION ordered_union",
                "CACHE TABLE left_table AS SELECT 1 AS id",
                "CACHE TABLE right_table AS SELECT 2 AS id",
                "CACHE TABLE result_table AS " +
                        "SELECT * FROM left_table UNION ALL SELECT * FROM right_table ORDER BY id",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertTrue(unionSources.get(0));
        assertTrue(unionSources.get(1));
        assertFalse(unionSources.get(2));
        assertFalse(unionSources.get(3));
    }

    @Test
    public void testExceptionAnalysisRecognizesUnionSubquery() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION nested_union",
                "CACHE TABLE left_table AS SELECT 1 AS id",
                "CACHE TABLE right_table AS SELECT 2 AS id",
                "CACHE TABLE result_table AS SELECT * FROM (" +
                        "SELECT * FROM left_table UNION ALL SELECT * FROM right_table) AS union_result",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertTrue(unionSources.get(0));
        assertTrue(unionSources.get(1));
        assertFalse(unionSources.get(2));
        assertFalse(unionSources.get(3));
    }

    @Test
    public void testExceptionAnalysisIgnoresSetControlEdges() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION union_after_set",
                "CACHE TABLE left_table AS SELECT 1 AS id",
                "SET marker=value",
                "CACHE TABLE right_table AS SELECT 2 AS id",
                "CACHE TABLE result_table AS " +
                        "SELECT * FROM left_table UNION ALL SELECT * FROM right_table",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertTrue(unionSources.get(0));
        assertFalse(unionSources.get(1));
        assertTrue(unionSources.get(2));
    }

    @Test
    public void testExceptionAnalysisDoesNotIgnoreSourceWithReturnedConsumer() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION shared_source_consumers",
                "CACHE TABLE source_table AS SELECT 1 AS id",
                "CACHE TABLE unused_table AS " +
                        "SELECT * FROM source_table UNION ALL SELECT * FROM source_table",
                "CACHE TABLE result_table AS SELECT * FROM source_table",
                "RETURN result_table"
        ));
        List<BindableInterface> bindableList = function.getBindableList();
        List<Integer> sorted = TopologicalSortUtils.topologicalSort(bindableList).getKey();

        Map<Integer, Boolean> unionSources = TopologicalSortUtils.getIsUnionSource(bindableList, sorted);

        assertFalse(unionSources.get(0), "one returned consumer makes the shared source non-ignorable");
        assertFalse(unionSources.get(1), "an unused UNION result is not a UNION branch source");
        assertFalse(unionSources.get(2));
        assertFalse(unionSources.get(3));
    }

    @Test
    public void testTopologicalSortWithSqlFunctionBindables() throws Exception {
        SqlFunctionBindable function = compileFunction(List.of(
                "CREATE SQL FUNCTION sorted_sql_bindables",
                "CACHE TABLE t1 AS SELECT 1 AS id",
                "CACHE TABLE t2 AS SELECT * FROM t1",
                "SELECT * FROM t2",
                "RETURN"
        ));

        Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> result =
                TopologicalSortUtils.topologicalSort(function.getBindableList());

        assertNotNull(result);
        assertNotNull(result.getKey());
        assertNotNull(result.getValue());
        assertEquals(4, result.getKey().size());
        assertTrue(result.getKey().indexOf(0) < result.getKey().indexOf(1));
        assertTrue(result.getKey().indexOf(1) < result.getKey().indexOf(2));
        assertEquals(Set.of(), result.getValue().get(0));
        assertEquals(Set.of(0), result.getValue().get(1));
        assertEquals(Set.of(1), result.getValue().get(2));
        assertEquals(Set.of(2), result.getValue().get(3));
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

        assertEquals(copy, original);
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

}
