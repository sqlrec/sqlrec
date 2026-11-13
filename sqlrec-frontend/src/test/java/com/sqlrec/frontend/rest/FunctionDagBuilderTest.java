package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.runtime.ProxyAllBindable;
import com.sqlrec.runtime.SetBindable;
import com.sqlrec.runtime.SqlFunctionBindable;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FunctionDagBuilderTest {
    @Test
    void preservesNodeFieldsProxyTypesAndDependencyDirection() {
        CalciteBindable sql = mock(CalciteBindable.class);
        when(sql.getName()).thenReturn("example:scan");
        when(sql.getSql()).thenReturn("select id from items");
        when(sql.getDependencySqlFuncName()).thenReturn(Set.of("lookup"));
        when(sql.getLogicalPlan()).thenReturn("logical");
        when(sql.getPhysicalPlan()).thenReturn("physical");
        when(sql.getJavaExpression()).thenReturn("expression");
        ProxyAllBindable cache = mock(ProxyAllBindable.class);
        when(cache.getDelegate()).thenReturn(mock(CacheTableBindable.class));
        when(cache.getCacheTableName()).thenReturn("cached_items");
        when(cache.getCacheTableDataFields()).thenReturn(List.of(
                DataTypeUtils.getRelDataTypeField("id", 0, SqlTypeName.BIGINT)));
        SetBindable assignment = mock(SetBindable.class);
        SqlFunctionBindable function = mock(SqlFunctionBindable.class);
        when(function.getBindableList()).thenReturn(List.of(sql, cache, assignment));
        when(function.getBindableDependency()).thenReturn(Map.of(1, Set.of(0)));

        Map<String, Object> result = FunctionDagBuilder.build("EXAMPLE", function);
        List<?> nodes = (List<?>) result.get("nodes");
        Map<?, ?> sqlNode = (Map<?, ?>) nodes.get(0);
        assertEquals(3, nodes.size());
        assertEquals("0", sqlNode.get("id"));
        assertEquals("sql", sqlNode.get("type"));
        assertEquals("scan", sqlNode.get("label"));
        assertEquals("select id from items", sqlNode.get("sql"));
        assertEquals("lookup", sqlNode.get("dependencyFunction"));
        assertEquals("logical", sqlNode.get("logicalPlan"));
        assertEquals("physical", sqlNode.get("physicalPlan"));
        assertEquals("expression", sqlNode.get("javaExpression"));
        assertEquals(-1.0, sqlNode.get("avgExecTimeMs"));
        assertEquals(-1.0, sqlNode.get("avgDataCount"));
        assertFalse(sqlNode.containsKey("cacheTableName"));
        Map<?, ?> cacheNode = (Map<?, ?>) nodes.get(1);
        assertEquals("cache", cacheNode.get("type"));
        assertEquals("cached_items", cacheNode.get("label"));
        assertEquals("cached_items", cacheNode.get("cacheTableName"));
        assertEquals(List.of(Map.of("name", "id", "type", "BIGINT NOT NULL")),
                cacheNode.get("cacheTableDataFields"));
        assertEquals("SET", ((Map<?, ?>) nodes.get(2)).get("label"));
        assertEquals(List.of(Map.of("id", "0-1", "source", "0", "target", "1")), result.get("edges"));
    }

    @Test
    void handlesEmptyFunctionsWithoutDependencyMap() {
        SqlFunctionBindable function = mock(SqlFunctionBindable.class);
        when(function.getBindableList()).thenReturn(List.of());
        when(function.getBindableDependency()).thenReturn(null);

        assertEquals(Map.of("nodes", List.of(), "edges", List.of()),
                FunctionDagBuilder.build("example", function));
    }

    @Test
    void averagesSuccessfulSamplesAcrossMetricTags() {
        String nodeName = "dag_metrics_test:scan";
        CompositeMeterRegistry registry = MetricsUtils.getCompositeMeterRegistry();
        SimpleMeterRegistry samples = new SimpleMeterRegistry();
        registry.add(samples);
        try {
            registry.timer(Consts.METRICS_NODE_EXEC_DURATION,
                    "name", nodeName, "outcome", "success", "group", "a").record(10, TimeUnit.MILLISECONDS);
            for (int i = 0; i < 3; i++) {
                registry.timer(Consts.METRICS_NODE_EXEC_DURATION,
                        "name", nodeName, "outcome", "success", "group", "b").record(30, TimeUnit.MILLISECONDS);
                registry.summary(Consts.METRICS_NODE_ROWS,
                        "name", nodeName, "outcome", "success", "group", "b").record(6);
            }
            registry.summary(Consts.METRICS_NODE_ROWS,
                    "name", nodeName, "outcome", "success", "group", "a").record(2);
            registry.timer(Consts.METRICS_NODE_EXEC_DURATION,
                    "name", nodeName, "outcome", "error").record(1000, TimeUnit.MILLISECONDS);
            registry.summary(Consts.METRICS_NODE_ROWS,
                    "name", nodeName, "outcome", "error").record(1000);
            BindableInterface node = mock(BindableInterface.class);
            when(node.getName()).thenReturn(nodeName);
            SqlFunctionBindable function = mock(SqlFunctionBindable.class);
            when(function.getBindableList()).thenReturn(List.of(node));

            List<?> nodes = (List<?>) FunctionDagBuilder.build("dag_metrics_test", function).get("nodes");
            Map<?, ?> result = (Map<?, ?>) nodes.get(0);
            assertEquals(25.0, (double) result.get("avgExecTimeMs"), 0.000001);
            assertEquals(5.0, (double) result.get("avgDataCount"), 0.000001);
        } finally {
            registry.getMeters().stream()
                    .filter(meter -> nodeName.equals(meter.getId().getTag("name")))
                    .toList().forEach(registry::remove);
            registry.remove(samples);
            samples.close();
        }
    }
}
