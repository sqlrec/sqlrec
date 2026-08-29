package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guard against forgotten forwarding overrides in wrapper bindables.
 * Dependency metadata lost in a wrapper layer used to break FunctionUpdater
 * refresh and circular dependency detection silently.
 */
public class BindableForwardingTest {

    private static class StubBindable extends BindableInterface {
        private final Set<String> dependencySqlFuncName;
        private final Set<String> dependencyJavaFuncName;
        private final Map<String, String> allDependSqlFunctionMap;
        private final String logicalPlan;

        StubBindable(
                Set<String> dependencySqlFuncName,
                Set<String> dependencyJavaFuncName,
                Map<String, String> allDependSqlFunctionMap,
                String logicalPlan
        ) {
            this.dependencySqlFuncName = dependencySqlFuncName;
            this.dependencyJavaFuncName = dependencyJavaFuncName;
            this.allDependSqlFunctionMap = allDependSqlFunctionMap;
            this.logicalPlan = logicalPlan;
        }

        @Override
        public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
            return Linq4j.emptyEnumerable();
        }

        @Override
        public List<RelDataTypeField> getReturnDataFields() {
            return Collections.singletonList(
                    DataTypeUtils.getRelDataTypeField("a", 0, SqlTypeName.INTEGER)
            );
        }

        @Override
        public boolean isParallelizable() {
            return true;
        }

        @Override
        public Set<String> getReadTables() {
            return new HashSet<>(Collections.singletonList("SRC"));
        }

        @Override
        public Set<String> getWriteTables() {
            return new HashSet<>();
        }

        @Override
        public boolean isUnionSql() {
            return true;
        }

        @Override
        public Set<String> getDependencySqlFuncName() {
            return dependencySqlFuncName;
        }

        @Override
        public Set<String> getDependencyJavaFuncName() {
            return dependencyJavaFuncName;
        }

        @Override
        public Map<String, String> getAllDependSqlFunctionMap() {
            return allDependSqlFunctionMap;
        }

        @Override
        public String getLogicalPlan() {
            return logicalPlan;
        }

        @Override
        public String getPhysicalPlan() {
            return logicalPlan == null ? null : logicalPlan + "-physical";
        }

        @Override
        public String getJavaExpression() {
            return logicalPlan == null ? null : logicalPlan + "-java";
        }
    }

    private CalciteBindable newConditionBindable() {
        return new CalciteBindable(
                new HashMap<>(),
                dataContext -> Linq4j.emptyEnumerable(),
                null, null, null, null, null
        );
    }

    @Test
    void cacheTableBindableForwardsDependencyAndPlanMetadata() {
        StubBindable inner = new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                "logical-a"
        );
        CacheTableBindable cacheTable = new CacheTableBindable("t", inner);

        assertEquals(Collections.singleton("FUN_A"), cacheTable.getDependencySqlFuncName());
        assertEquals(Collections.singleton("JF_X"), cacheTable.getDependencyJavaFuncName());
        assertEquals(Collections.singletonMap("FUN_C", "FUN_A->FUN_C"), cacheTable.getAllDependSqlFunctionMap());
        assertEquals(Collections.singleton("SRC"), cacheTable.getReadTables());
        assertEquals(Collections.singleton("t"), cacheTable.getWriteTables());
        assertTrue(cacheTable.isParallelizable());
        assertTrue(cacheTable.isUnionSql());
        assertEquals("logical-a", cacheTable.getLogicalPlan());
        assertEquals("logical-a-physical", cacheTable.getPhysicalPlan());
        assertEquals("logical-a-java", cacheTable.getJavaExpression());
    }

    @Test
    void ifCacheBindableMergesBothBranchDependencies() {
        StubBindable thenInner = new StubBindable(
                new HashSet<>(Arrays.asList("FUN_A", "FUN_SHARED")),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                null
        );
        StubBindable elseInner = new StubBindable(
                new HashSet<>(Arrays.asList("FUN_B", "FUN_SHARED")),
                Collections.singleton("JF_Y"),
                Collections.singletonMap("FUN_D", "FUN_B->FUN_D"),
                null
        );
        IfBindable ifCache = new IfBindable(
                newConditionBindable(),
                new CacheTableBindable("t", thenInner),
                new CacheTableBindable("t", elseInner),
                false
        );

        assertEquals(
                new HashSet<>(Arrays.asList("FUN_A", "FUN_B", "FUN_SHARED")),
                ifCache.getDependencySqlFuncName()
        );
        assertEquals(new HashSet<>(Arrays.asList("JF_X", "JF_Y")), ifCache.getDependencyJavaFuncName());
        Map<String, String> expectedAllDepend = new HashMap<>();
        expectedAllDepend.put("FUN_C", "FUN_A->FUN_C");
        expectedAllDepend.put("FUN_D", "FUN_B->FUN_D");
        assertEquals(expectedAllDepend, ifCache.getAllDependSqlFunctionMap());
    }

    @Test
    void ifCacheBindableWithoutElseKeepsThenDependency() {
        StubBindable thenInner = new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.emptySet(),
                Collections.emptyMap(),
                null
        );
        IfBindable ifCache = new IfBindable(
                newConditionBindable(),
                new CacheTableBindable("t", thenInner),
                null,
                false
        );

        assertEquals(Collections.singleton("FUN_A"), ifCache.getDependencySqlFuncName());
        assertEquals(Collections.emptySet(), ifCache.getDependencyJavaFuncName());
        assertEquals(Collections.emptyMap(), ifCache.getAllDependSqlFunctionMap());
    }

    @Test
    void proxyAllBindableForwardsDependencyAndPlanMetadata() {
        StubBindable inner = new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                "logical-a"
        );
        ProxyAllBindable proxy = new ProxyAllBindable(inner);

        assertEquals(Collections.singleton("FUN_A"), proxy.getDependencySqlFuncName());
        assertEquals(Collections.singleton("JF_X"), proxy.getDependencyJavaFuncName());
        assertEquals(Collections.singletonMap("FUN_C", "FUN_A->FUN_C"), proxy.getAllDependSqlFunctionMap());
        assertEquals(Collections.singleton("SRC"), proxy.getReadTables());
        assertTrue(proxy.isParallelizable());
        assertTrue(proxy.isUnionSql());
        assertEquals("logical-a", proxy.getLogicalPlan());
        assertEquals("logical-a-physical", proxy.getPhysicalPlan());
        assertEquals("logical-a-java", proxy.getJavaExpression());
    }

    @Test
    void wrapperChainKeepsDependencyEndToEnd() {
        // simulates a function body node: proxy -> cache table -> call function
        StubBindable callNode = new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                null
        );
        ProxyAllBindable node = new ProxyAllBindable(new CacheTableBindable("t", callNode));

        assertEquals(Collections.singleton("FUN_A"), node.getDependencySqlFuncName());
        assertEquals(Collections.singleton("JF_X"), node.getDependencyJavaFuncName());
        assertEquals(Collections.singletonMap("FUN_C", "FUN_A->FUN_C"), node.getAllDependSqlFunctionMap());
    }

    @Test
    void returnBindableForwardsDependencyAndPlanMetadata() {
        StubBindable inner = new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                "logical-a"
        );
        ReturnBindable returnBindable = new ReturnBindable(inner);

        assertTrue(returnBindable.containsReturn());
        assertEquals(Collections.singleton("FUN_A"), returnBindable.getDependencySqlFuncName());
        assertEquals(Collections.singleton("JF_X"), returnBindable.getDependencyJavaFuncName());
        assertEquals(Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                returnBindable.getAllDependSqlFunctionMap());
        assertEquals(Collections.singleton("SRC"), returnBindable.getReadTables());
        assertEquals("logical-a", returnBindable.getLogicalPlan());
        assertEquals("logical-a-physical", returnBindable.getPhysicalPlan());
        assertEquals("logical-a-java", returnBindable.getJavaExpression());
    }

    @Test
    void ifReturnMergesDependenciesFromBothBranches() {
        ReturnBindable thenReturn = new ReturnBindable(new StubBindable(
                Collections.singleton("FUN_A"),
                Collections.singleton("JF_X"),
                Collections.singletonMap("FUN_C", "FUN_A->FUN_C"),
                null
        ));
        ReturnBindable elseReturn = new ReturnBindable(new StubBindable(
                Collections.singleton("FUN_B"),
                Collections.singleton("JF_Y"),
                Collections.singletonMap("FUN_D", "FUN_B->FUN_D"),
                null
        ));
        IfBindable ifReturn = new IfBindable(
                newConditionBindable(),
                thenReturn,
                elseReturn,
                false
        );

        assertTrue(ifReturn.containsReturn());
        assertEquals(new HashSet<>(Arrays.asList("FUN_A", "FUN_B")),
                ifReturn.getDependencySqlFuncName());
        assertEquals(new HashSet<>(Arrays.asList("JF_X", "JF_Y")),
                ifReturn.getDependencyJavaFuncName());
        assertEquals(new HashSet<>(Collections.singletonList("SRC")), ifReturn.getReadTables());
        Map<String, String> expected = new HashMap<>();
        expected.put("FUN_C", "FUN_A->FUN_C");
        expected.put("FUN_D", "FUN_B->FUN_D");
        assertEquals(expected, ifReturn.getAllDependSqlFunctionMap());
    }

    @Test
    void proxyForwardsContainsReturn() {
        ProxyAllBindable proxy = new ProxyAllBindable(new ReturnBindable(new StubBindable(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptyMap(),
                null
        )));

        assertTrue(proxy.containsReturn());
    }
}
