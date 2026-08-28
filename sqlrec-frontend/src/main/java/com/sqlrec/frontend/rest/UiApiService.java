package com.sqlrec.frontend.rest;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.runtime.FunctionProxyBindable;
import com.sqlrec.runtime.IfBindable;
import com.sqlrec.runtime.ProxyAllBindable;
import com.sqlrec.runtime.SetBindable;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.utils.ModelUtils;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

final class UiApiService {
    private static final Logger logger = LoggerFactory.getLogger(UiApiService.class);

    private final MetadataAccess metadataAccess;

    UiApiService() {
        this(MetadataAccessFactory.getInstance());
    }

    UiApiService(MetadataAccess metadataAccess) {
        this.metadataAccess = Objects.requireNonNull(metadataAccess, "metadataAccess");
    }

    List<Map<String, Object>> listFunctions() {
        return toItems(metadataAccess.getSqlFunctionList(), SqlFunction::getName);
    }

    List<Map<String, String>> getFunction(String name) {
        SqlFunction function = metadataAccess.getSqlFunction(name);
        return function == null ? null : convertFunctionToTable(function);
    }

    Map<String, Object> getFunctionDag(String functionName) throws Exception {
        SqlFunctionBindable sqlFunctionBindable = new CompileManager().getSqlFunction(functionName);
        if (sqlFunctionBindable == null) {
            return null;
        }

        String functionNamePrefix = ResourceNames.normalize(functionName) + ":";
        List<BindableInterface> bindables = sqlFunctionBindable.getBindableList();
        Map<Integer, Set<Integer>> dependencies = sqlFunctionBindable.getBindableDependency();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        for (int i = 0; i < bindables.size(); i++) {
            BindableInterface bindable = bindables.get(i);
            String originalName = getBindableLabel(bindable, i);
            Map<String, Object> node = new HashMap<>();
            node.put("id", String.valueOf(i));
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
            nodes.add(node);
        }

        if (dependencies != null) {
            for (Map.Entry<Integer, Set<Integer>> entry : dependencies.entrySet()) {
                int targetId = entry.getKey();
                for (int sourceId : entry.getValue()) {
                    Map<String, Object> edge = new HashMap<>();
                    edge.put("id", sourceId + "-" + targetId);
                    edge.put("source", String.valueOf(sourceId));
                    edge.put("target", String.valueOf(targetId));
                    edges.add(edge);
                }
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodes);
        result.put("edges", edges);
        return result;
    }

    List<Map<String, Object>> listDatabases() throws Exception {
        return toItems(metadataAccess.getDatabases(), Function.identity());
    }

    List<Map<String, Object>> listTables(String database) throws Exception {
        return metadataAccess.getTables(database).stream()
                .map(table -> {
                    Map<String, Object> item = new HashMap<>();
                    item.put("id", table.getTableName());
                    item.put("name", table.getTableName());
                    item.put("database", table.getDbName());
                    item.put("owner", table.getOwner());
                    item.put("tableType", table.getTableType());
                    item.put("createTime", formatTimestamp(table.getCreateTime() * 1000L));
                    return item;
                })
                .collect(Collectors.toList());
    }

    Map<String, Object> getTable(String database, String tableName) throws Exception {
        org.apache.hadoop.hive.metastore.api.Table table = metadataAccess.getTable(database, tableName);
        if (table == null) {
            return null;
        }

        List<Map<String, String>> rows = new ArrayList<>();
        List<FieldSchema> columns = HiveTableUtils.parse(table);
        rows.add(createRow("# Columns", ""));
        rows.add(createRow("Name", "Type"));
        for (FieldSchema column : columns) {
            rows.add(createRow(column.getName(), column.getType()));
        }
        rows.add(createRow("", ""));

        if (table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty()) {
            rows.add(createRow("# Partition Keys", ""));
            rows.add(createRow("Name", "Type"));
            for (org.apache.hadoop.hive.metastore.api.FieldSchema partitionKey : table.getPartitionKeys()) {
                rows.add(createRow(partitionKey.getName(), partitionKey.getType()));
            }
            rows.add(createRow("", ""));
        }

        Map<String, Object> result = new HashMap<>();
        result.put("tableData", rows);
        return result;
    }

    List<Map<String, Object>> listApis() {
        return toItems(metadataAccess.getSqlApiList(), SqlApi::getName);
    }

    Map<String, Object> getApi(String name) {
        SqlApi api = metadataAccess.getSqlApi(name);
        if (api == null) {
            return null;
        }

        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# API Information", ""));
        rows.add(createRow("API Name:", api.getName()));
        rows.add(createRow("Function Name:", api.getFunctionName()));
        rows.add(createRow("Created At:", formatTimestamp(api.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(api.getUpdatedAt())));

        Map<String, Object> result = new HashMap<>();
        result.put("tableData", rows);
        return result;
    }

    List<Map<String, Object>> listModels() {
        return toItems(metadataAccess.getModelList(), Model::getName);
    }

    Map<String, Object> getModel(String name) throws Exception {
        Model model = metadataAccess.getModel(name);
        if (model == null) {
            return null;
        }

        List<List<String>> rows = new ArrayList<>();
        ModelUtils.addModelInfo(rows, model);
        Map<String, Object> result = new HashMap<>();
        result.put("tableData", convertRowsToMap(rows));
        if (model.getDdl() != null) {
            result.put("ddl", model.getDdl());
        }
        return result;
    }

    Map<String, Object> listCheckpoints(String modelName, int page, int pageSize) {
        int total = metadataAccess.getCheckpointCountByModelName(modelName);
        List<Map<String, Object>> items = metadataAccess
                .getCheckpointListByModelNamePaged(modelName, page, pageSize)
                .stream()
                .map(checkpoint -> {
                    Map<String, Object> item = new HashMap<>();
                    item.put("checkpointName", checkpoint.getCheckpointName());
                    item.put("checkpointType", checkpoint.getCheckpointType());
                    item.put("status", checkpoint.getStatus());
                    item.put("createdAt", formatTimestamp(checkpoint.getCreatedAt()));
                    item.put("updatedAt", formatTimestamp(checkpoint.getUpdatedAt()));
                    return item;
                })
                .collect(Collectors.toList());

        Map<String, Object> result = new HashMap<>();
        result.put("items", items);
        result.put("total", total);
        result.put("page", page);
        result.put("pageSize", pageSize);
        result.put("totalPages", (int) Math.ceil((double) total / pageSize));
        return result;
    }

    Map<String, Object> getCheckpoint(String modelName, String checkpointName) {
        Checkpoint checkpoint = metadataAccess.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("modelName", checkpoint.getModelName());
        result.put("checkpointName", checkpoint.getCheckpointName());
        result.put("checkpointType", checkpoint.getCheckpointType());
        result.put("status", checkpoint.getStatus());
        result.put("ddl", checkpoint.getDdl());
        result.put("modelDdl", checkpoint.getModelDdl());
        result.put("yaml", checkpoint.getYaml());
        result.put("createdAt", formatTimestamp(checkpoint.getCreatedAt()));
        result.put("updatedAt", formatTimestamp(checkpoint.getUpdatedAt()));
        return result;
    }

    List<Map<String, Object>> listServices() {
        return toItems(metadataAccess.getServiceList(), Service::getName);
    }

    Map<String, Object> getService(String name) throws Exception {
        Service service = metadataAccess.getService(name);
        if (service == null) {
            return null;
        }

        List<List<String>> rows = new ArrayList<>();
        ModelUtils.addServiceInfo(rows, service);
        Map<String, Object> result = new HashMap<>();
        result.put("tableData", convertRowsToMap(rows));
        if (service.getYaml() != null) {
            result.put("yaml", service.getYaml());
        }
        if (service.getDdl() != null) {
            result.put("ddl", service.getDdl());
        }
        return result;
    }

    private static <T> List<Map<String, Object>> toItems(List<T> values, Function<T, String> nameFunction) {
        return values.stream()
                .map(value -> {
                    String name = nameFunction.apply(value);
                    Map<String, Object> item = new HashMap<>();
                    item.put("id", name);
                    item.put("name", name);
                    return item;
                })
                .collect(Collectors.toList());
    }

    private static List<Map<String, String>> convertFunctionToTable(SqlFunction function) {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# Function Information", ""));
        rows.add(createRow("Function Name:", function.getName()));
        rows.add(createRow("Created At:", formatTimestamp(function.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(function.getUpdatedAt())));
        rows.add(createRow("", ""));
        rows.add(createRow("# SQL Statements", ""));
        if (function.getSqlList() == null || function.getSqlList().isEmpty()) {
            rows.add(createRow("(none)", ""));
            return rows;
        }

        String[] sqlStatements = function.getSqlList().split(";");
        for (int i = 0; i < sqlStatements.length; i++) {
            String sql = sqlStatements[i].trim();
            if (!sql.isEmpty()) {
                rows.add(createRow("SQL " + (i + 1) + ":", sql));
            }
        }
        return rows;
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
            Tags tags = Tags.of("name", nodeName, "status", "success");
            Timer timer = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_EXEC_DURATION)
                    .tags(tags)
                    .timer();
            return timer != null && timer.count() > 0
                    ? timer.mean(TimeUnit.MILLISECONDS)
                    : -1;
        } catch (Exception e) {
            logger.debug("Failed to get avg exec time for node: {}", nodeName);
            return -1;
        }
    }

    private static double getNodeAvgDataCount(String nodeName) {
        try {
            Tags tags = Tags.of("name", nodeName, "status", "success");
            DistributionSummary summary = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_DATA_SIZE)
                    .tags(tags)
                    .summary();
            return summary != null && summary.count() > 0 ? summary.mean() : -1;
        } catch (Exception e) {
            logger.debug("Failed to get avg data count for node: {}", nodeName);
            return -1;
        }
    }

    private static List<Map<String, String>> convertRowsToMap(List<List<String>> rows) {
        return rows.stream()
                .map(row -> createRow(row.get(0), row.size() > 1 ? row.get(1) : ""))
                .collect(Collectors.toList());
    }

    private static Map<String, String> createRow(String columnName, String dataType) {
        Map<String, String> row = new HashMap<>();
        row.put("col_name", columnName);
        row.put("data_type", dataType);
        return row;
    }

    private static String formatTimestamp(long timestamp) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new java.util.Date(timestamp));
    }
}
