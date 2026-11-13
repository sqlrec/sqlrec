package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.runtime.FunctionProxyBindable;
import com.sqlrec.runtime.IfBindable;
import com.sqlrec.runtime.ProxyAllBindable;
import com.sqlrec.runtime.ReturnBindable;
import com.sqlrec.runtime.SetBindable;
import com.sqlrec.runtime.SqlFunctionBindable;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class FunctionDagBuilder {
    private static final Logger logger = LoggerFactory.getLogger(FunctionDagBuilder.class);

    private FunctionDagBuilder() {
    }

    static Map<String, Object> build(String functionName, SqlFunctionBindable sqlFunctionBindable) {
        String functionNamePrefix = ResourceNames.normalize(functionName) + ":";
        List<BindableInterface> bindables = sqlFunctionBindable.getBindableList();
        Map<Integer, Set<Integer>> dependencies = sqlFunctionBindable.getBindableDependency();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        for (int i = 0; i < bindables.size(); i++) {
            nodes.add(createDagNode(bindables.get(i), i, functionNamePrefix));
        }

        if (dependencies != null) {
            for (Map.Entry<Integer, Set<Integer>> entry : dependencies.entrySet()) {
                int targetId = entry.getKey();
                for (int sourceId : entry.getValue()) {
                    edges.add(createDagEdge(sourceId, targetId));
                }
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodes);
        result.put("edges", edges);
        return result;
    }

    private static Map<String, Object> createDagNode(
            BindableInterface bindable,
            int index,
            String functionNamePrefix
    ) {
        String originalName = getBindableLabel(bindable, index);
        Map<String, Object> node = new HashMap<>();
        node.put("id", String.valueOf(index));
        node.put("type", getBindableType(bindable));
        node.put("label", stripFunctionNamePrefix(originalName, functionNamePrefix));
        node.put("sql", bindable.getSql());
        node.put("dependencyFunction", String.join(",", bindable.getDependencySqlFuncName()));
        node.put("avgExecTimeMs", getNodeAvgExecTime(originalName));
        node.put("avgDataCount", getNodeAvgDataCount(originalName));
        node.put("logicalPlan", bindable.getLogicalPlan());
        node.put("physicalPlan", bindable.getPhysicalPlan());
        node.put("javaExpression", bindable.getJavaExpression());
        addCacheTableFields(node, bindable);
        return node;
    }

    private static Map<String, Object> createDagEdge(int sourceId, int targetId) {
        Map<String, Object> edge = new HashMap<>();
        edge.put("id", sourceId + "-" + targetId);
        edge.put("source", String.valueOf(sourceId));
        edge.put("target", String.valueOf(targetId));
        return edge;
    }

    private static void addCacheTableFields(Map<String, Object> node, BindableInterface bindable) {
        String cacheTableName = bindable.getCacheTableName();
        List<RelDataTypeField> dataFields = bindable.getCacheTableDataFields();
        if (cacheTableName == null || cacheTableName.isEmpty()
                || dataFields == null || dataFields.isEmpty()) {
            return;
        }

        node.put("cacheTableName", cacheTableName);
        List<Map<String, String>> fields = new ArrayList<>();
        for (RelDataTypeField field : dataFields) {
            Map<String, String> fieldMap = new HashMap<>();
            fieldMap.put("name", field.getName());
            fieldMap.put("type", field.getType().getFullTypeString());
            fields.add(fieldMap);
        }
        node.put("cacheTableDataFields", fields);
    }

    private static String getBindableType(BindableInterface bindable) {
        if (bindable instanceof ProxyAllBindable proxyAllBindable) {
            return getBindableType(proxyAllBindable.getDelegate());
        }
        if (bindable instanceof FunctionProxyBindable) {
            return "function";
        }
        if (bindable instanceof CacheTableBindable) {
            return "cache";
        }
        if (bindable instanceof CalciteBindable) {
            return "sql";
        }
        if (bindable instanceof IfBindable) {
            return "condition";
        }
        if (bindable instanceof SetBindable) {
            return "set";
        }
        if (bindable instanceof ReturnBindable) {
            return "return";
        }
        return "unknown";
    }

    private static String getBindableLabel(BindableInterface bindable, int index) {
        String name = bindable.getName();
        if (name != null && !name.isEmpty()) {
            return name;
        }

        String cacheTableName = bindable.getCacheTableName();
        if (cacheTableName != null && !cacheTableName.isEmpty()) {
            return cacheTableName;
        }
        if (bindable instanceof CacheTableBindable cacheTableBindable) {
            return cacheTableBindable.getTableName();
        }
        if (bindable instanceof SetBindable) {
            return "SET";
        }
        return "Node " + index;
    }

    private static String stripFunctionNamePrefix(String name, String prefix) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return name.startsWith(prefix) ? name.substring(prefix.length()) : name;
    }

    private static double getNodeAvgExecTime(String nodeName) {
        try {
            Tags tags = Tags.of("name", nodeName, "outcome", "success");
            java.util.Collection<Timer> timers = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_EXEC_DURATION)
                    .tags(tags)
                    .timers();
            long count = timers.stream().mapToLong(Timer::count).sum();
            double total = timers.stream().mapToDouble(timer -> timer.totalTime(TimeUnit.MILLISECONDS)).sum();
            return count > 0 ? total / count : -1;
        } catch (Exception e) {
            logger.debug("Failed to get avg exec time for node: {}", nodeName);
            return -1;
        }
    }

    private static double getNodeAvgDataCount(String nodeName) {
        try {
            Tags tags = Tags.of("name", nodeName, "outcome", "success");
            java.util.Collection<DistributionSummary> summaries = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_ROWS)
                    .tags(tags)
                    .summaries();
            long count = summaries.stream().mapToLong(DistributionSummary::count).sum();
            double total = summaries.stream().mapToDouble(DistributionSummary::totalAmount).sum();
            return count > 0 ? total / count : -1;
        } catch (Exception e) {
            logger.debug("Failed to get avg data count for node: {}", nodeName);
            return -1;
        }
    }
}
