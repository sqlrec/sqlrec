package com.sqlrec.frontend.RestService;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.*;
import com.sqlrec.frontend.common.CommonUtils;
import com.sqlrec.runtime.*;
import com.sqlrec.utils.DbUtils;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class UiHandler {
    private static final Logger logger = LoggerFactory.getLogger(UiHandler.class);
    private static final String STATIC_PREFIX = "/ui/static/";
    private static final String API_PREFIX = "/ui/api/";

    public static FullHttpResponse handleRequest(String uri, HttpMethod method, String postData) {
        try {
            if (uri.startsWith(STATIC_PREFIX)) {
                return handleStaticResource(uri);
            } else if (uri.startsWith(API_PREFIX)) {
                return handleApiRequest(uri, method, postData);
            } else {
                return createErrorResponse(HttpResponseStatus.NOT_FOUND, "UI path not found");
            }
        } catch (Exception e) {
            logger.error("Error handling UI request: {}", uri, e);
            return createErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private static FullHttpResponse handleStaticResource(String uri) {
        String resourcePath = uri.substring(STATIC_PREFIX.length());

        if (resourcePath.isEmpty()) {
            return createErrorResponse(HttpResponseStatus.BAD_REQUEST, "Resource path is empty");
        }

        try {
            InputStream inputStream = UiHandler.class.getClassLoader()
                    .getResourceAsStream("ui/static/" + resourcePath);

            if (inputStream == null) {
                logger.warn("Static resource not found: {}, returning index.html", resourcePath);
                inputStream = UiHandler.class.getClassLoader()
                        .getResourceAsStream("ui/static/index.html");

                if (inputStream == null) {
                    logger.error("index.html not found in jar");
                    return createErrorResponse(HttpResponseStatus.NOT_FOUND, "index.html not found");
                }

                resourcePath = "index.html";
            }

            byte[] content = inputStream.readAllBytes();
            inputStream.close();

            String contentType = getContentType(resourcePath);
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(content)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);

            if (resourcePath.equals("index.html")) {
                response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
            } else {
                response.headers().set(HttpHeaderNames.CACHE_CONTROL, "max-age=86400");
            }

            return response;
        } catch (Exception e) {
            logger.error("Error reading static resource: {}", resourcePath, e);
            return createErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to read resource");
        }
    }

    private static FullHttpResponse handleApiRequest(String uri, HttpMethod method, String postData) {
        String apiPath = uri.substring(API_PREFIX.length());

        if (apiPath.isEmpty()) {
            return createErrorResponse(HttpResponseStatus.BAD_REQUEST, "API path is empty");
        }

        try {
            Object result = null;

            if (apiPath.equals("functions")) {
                List<SqlFunction> functions = DbUtils.getSqlFunctionList();
                result = functions.stream()
                        .map(f -> {
                            Map<String, Object> item = new HashMap<>();
                            item.put("id", f.getName());
                            item.put("name", f.getName());
                            return item;
                        })
                        .collect(Collectors.toList());
            } else if (apiPath.startsWith("functions/")) {
                String name = apiPath.substring("functions/".length());
                SqlFunction function = DbUtils.getSqlFunction(name);
                if (function == null) {
                    return createErrorResponse(HttpResponseStatus.NOT_FOUND, "Function not found: " + name);
                }
                result = convertFunctionToTable(function);
            } else if (apiPath.startsWith("functions-dag/")) {
                String name = apiPath.substring("functions-dag/".length());
                result = getFunctionDag(name);
            } else if (apiPath.equals("apis")) {
                List<SqlApi> apis = DbUtils.getSqlApiList();
                result = apis.stream()
                        .map(a -> {
                            Map<String, Object> item = new HashMap<>();
                            item.put("id", a.getName());
                            item.put("name", a.getName());
                            return item;
                        })
                        .collect(Collectors.toList());
            } else if (apiPath.startsWith("apis/")) {
                String name = apiPath.substring("apis/".length());
                SqlApi api = DbUtils.getSqlApi(name);
                if (api == null) {
                    return createErrorResponse(HttpResponseStatus.NOT_FOUND, "API not found: " + name);
                }
                result = convertApiToTable(api);
            } else if (apiPath.equals("models")) {
                List<Model> models = DbUtils.getModelList();
                result = models.stream()
                        .map(m -> {
                            Map<String, Object> item = new HashMap<>();
                            item.put("id", m.getName());
                            item.put("name", m.getName());
                            return item;
                        })
                        .collect(Collectors.toList());
            } else if (apiPath.startsWith("models/")) {
                String subPath = apiPath.substring("models/".length());
                int queryIndex = subPath.indexOf('?');
                String pathWithoutQuery = queryIndex > 0 ? subPath.substring(0, queryIndex) : subPath;
                if (pathWithoutQuery.contains("/checkpoints/")) {
                    String[] parts = pathWithoutQuery.split("/checkpoints/");
                    if (parts.length == 2) {
                        String modelName = parts[0];
                        String checkpointName = parts[1];
                        result = getCheckpointDetail(modelName, checkpointName);
                    } else {
                        return createErrorResponse(HttpResponseStatus.BAD_REQUEST, "Invalid checkpoint path");
                    }
                } else if (pathWithoutQuery.endsWith("/checkpoints")) {
                    String modelName = pathWithoutQuery.replace("/checkpoints", "");
                    result = getCheckpointListPaged(modelName, uri);
                } else {
                    Model model = DbUtils.getModel(pathWithoutQuery);
                    if (model == null) {
                        return createErrorResponse(HttpResponseStatus.NOT_FOUND, "Model not found: " + pathWithoutQuery);
                    }
                    result = convertModelToTable(model);
                }
            } else if (apiPath.equals("services")) {
                List<Service> services = DbUtils.getServiceList();
                result = services.stream()
                        .map(s -> {
                            Map<String, Object> item = new HashMap<>();
                            item.put("id", s.getName());
                            item.put("name", s.getName());
                            return item;
                        })
                        .collect(Collectors.toList());
            } else if (apiPath.startsWith("services/")) {
                String name = apiPath.substring("services/".length());
                Service service = DbUtils.getService(name);
                if (service == null) {
                    return createErrorResponse(HttpResponseStatus.NOT_FOUND, "Service not found: " + name);
                }
                result = convertServiceToTable(service);
            } else {
                return createErrorResponse(HttpResponseStatus.NOT_FOUND, "API not found: " + apiPath);
            }

            String jsonContent = JsonUtils.toJson(result);
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(jsonContent, CharsetUtil.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            return response;
        } catch (Exception e) {
            logger.error("Error handling API request: {}", apiPath, e);
            return createErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to process request: " + e.getMessage());
        }
    }

    private static List<Map<String, String>> convertFunctionToTable(SqlFunction function) {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# Function Information", ""));
        rows.add(createRow("Function Name:", function.getName()));
        rows.add(createRow("Created At:", formatTimestamp(function.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(function.getUpdatedAt())));
        rows.add(createRow("", ""));
        rows.add(createRow("# SQL Statements", ""));
        if (function.getSqlList() != null && !function.getSqlList().isEmpty()) {
            String[] sqls = function.getSqlList().split(";");
            for (int i = 0; i < sqls.length; i++) {
                if (!sqls[i].trim().isEmpty()) {
                    rows.add(createRow("SQL " + (i + 1) + ":", sqls[i].trim()));
                }
            }
        } else {
            rows.add(createRow("(none)", ""));
        }
        return rows;
    }

    private static Map<String, Object> getFunctionDag(String functionName) throws Exception {
        SqlFunctionBindable sqlFunctionBindable = new CompileManager().getSqlFunction(functionName);
        String funNamePrefix = functionName.toUpperCase() + ":";

        List<BindableInterface> bindableList = sqlFunctionBindable.getBindableList();
        Map<Integer, Set<Integer>> bindableDependency = sqlFunctionBindable.getBindableDependency();

        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);
            String originalName = getBindableLabel(bindable, i);
            Map<String, Object> node = new HashMap<>();
            node.put("id", String.valueOf(i));
            node.put("type", getBindableType(bindable));
            node.put("label", stripFunNamePrefix(originalName, funNamePrefix));
            node.put("sql", bindable.getSql());
            node.put("dependencyFunction", bindable.getDependencySqlFuncName());
            node.put("avgExecTimeMs", getNodeAvgExecTime(originalName));
            node.put("avgDataCount", getNodeAvgDataCount(originalName));
            node.put("logicalPlan", bindable.getLogicalPlan());
            node.put("physicalPlan", bindable.getPhysicalPlan());
            node.put("javaExpression", bindable.getJavaExpression());
            nodes.add(node);
        }

        if (bindableDependency != null) {
            for (Map.Entry<Integer, Set<Integer>> entry : bindableDependency.entrySet()) {
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

    private static String getBindableType(BindableInterface bindable) {
        if (bindable instanceof ProxyAllBindable) {
            return getBindableType(((ProxyAllBindable) bindable).getDelegate());
        } else if (bindable instanceof CacheTableBindable) {
            return "cache";
        } else if (bindable instanceof CalciteBindable) {
            return "sql";
        } else if (bindable instanceof CallSqlFunctionBindable) {
            return "function";
        } else if (bindable instanceof IfCacheBindable) {
            return "condition";
        } else if (bindable instanceof SetBindable) {
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

        if (bindable instanceof CacheTableBindable) {
            return ((CacheTableBindable) bindable).getTableName();
        } else if (bindable instanceof SetBindable) {
            return "SET";
        }

        return "Node " + index;
    }

    private static String stripFunNamePrefix(String name, String prefix) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        if (name.startsWith(prefix)) {
            return name.substring(prefix.length());
        }
        return name;
    }

    private static double getNodeAvgExecTime(String nodeName) {
        try {
            Tags tags = Tags.of("name", nodeName, "status", "success");
            Timer timer = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_EXEC_DURATION)
                    .tags(tags)
                    .timer();
            if (timer != null && timer.count() > 0) {
                return timer.mean(TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            logger.debug("Failed to get avg exec time for node: {}", nodeName);
        }
        return -1;
    }

    private static double getNodeAvgDataCount(String nodeName) {
        try {
            Tags tags = Tags.of("name", nodeName, "status", "success");
            DistributionSummary summary = MetricsUtils.getCompositeMeterRegistry()
                    .find(Consts.METRICS_NODE_DATA_SIZE)
                    .tags(tags)
                    .summary();
            if (summary != null && summary.count() > 0) {
                return summary.mean();
            }
        } catch (Exception e) {
            logger.debug("Failed to get avg data count for node: {}", nodeName);
        }
        return -1;
    }

    private static List<Map<String, String>> convertApiToTable(SqlApi api) {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# API Information", ""));
        rows.add(createRow("API Name:", api.getName()));
        rows.add(createRow("Function Name:", api.getFunctionName()));
        rows.add(createRow("Created At:", formatTimestamp(api.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(api.getUpdatedAt())));
        return rows;
    }

    private static List<Map<String, String>> convertModelToTable(Model model) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        CommonUtils.addModelInfo(rows, model);
        return convertRowsToMap(rows);
    }

    private static Map<String, Object> getCheckpointListPaged(String modelName, String uri) {
        int page = 1;
        int pageSize = 10;

        int queryIndex = uri.indexOf('?');
        if (queryIndex > 0) {
            String queryString = uri.substring(queryIndex + 1);
            String[] params = queryString.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=");
                if (keyValue.length == 2) {
                    if (keyValue[0].equals("page")) {
                        page = Integer.parseInt(keyValue[1]);
                    } else if (keyValue[0].equals("pageSize")) {
                        pageSize = Integer.parseInt(keyValue[1]);
                    }
                }
            }
        }

        int total = DbUtils.getCheckpointCountByModelName(modelName);
        List<Checkpoint> checkpoints = DbUtils.getCheckpointListByModelNamePaged(modelName, page, pageSize);

        List<Map<String, Object>> items = checkpoints.stream()
                .map(c -> {
                    Map<String, Object> item = new HashMap<>();
                    item.put("checkpointName", c.getCheckpointName());
                    item.put("checkpointType", c.getCheckpointType());
                    item.put("status", c.getStatus());
                    item.put("createdAt", formatTimestamp(c.getCreatedAt()));
                    item.put("updatedAt", formatTimestamp(c.getUpdatedAt()));
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

    private static Map<String, Object> getCheckpointDetail(String modelName, String checkpointName) {
        Checkpoint checkpoint = DbUtils.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Checkpoint not found: " + modelName + "/" + checkpointName);
            return error;
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

    private static List<Map<String, String>> convertServiceToTable(Service service) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        CommonUtils.addServiceInfo(rows, service);
        return convertRowsToMap(rows);
    }

    private static List<Map<String, String>> convertRowsToMap(List<List<String>> rows) {
        return rows.stream()
                .map(row -> createRow(row.get(0), row.size() > 1 ? row.get(1) : ""))
                .collect(Collectors.toList());
    }

    private static Map<String, String> createRow(String colName, String dataType) {
        Map<String, String> row = new HashMap<>();
        row.put("col_name", colName);
        row.put("data_type", dataType);
        return row;
    }

    private static String formatTimestamp(long timestamp) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp));
    }

    private static String getContentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=UTF-8";
        } else if (path.endsWith(".css")) {
            return "text/css; charset=UTF-8";
        } else if (path.endsWith(".js")) {
            return "application/javascript; charset=UTF-8";
        } else if (path.endsWith(".json")) {
            return "application/json; charset=UTF-8";
        } else if (path.endsWith(".png")) {
            return "image/png";
        } else if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
            return "image/jpeg";
        } else if (path.endsWith(".svg")) {
            return "image/svg+xml";
        } else if (path.endsWith(".ico")) {
            return "image/x-icon";
        } else if (path.endsWith(".woff") || path.endsWith(".woff2")) {
            return "font/woff2";
        } else if (path.endsWith(".ttf")) {
            return "font/ttf";
        } else {
            return "application/octet-stream";
        }
    }

    private static FullHttpResponse createErrorResponse(HttpResponseStatus status, String message) {
        Map<String, String> errorMap = new HashMap<>();
        errorMap.put("error", message);
        String jsonContent = JsonUtils.toJson(errorMap);

        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(jsonContent, CharsetUtil.UTF_8)
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        return response;
    }
}
