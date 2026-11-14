package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.frontend.utils.RestUtils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UiHandler {
    private static final int DEFAULT_PAGE = 1;
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;

    private final UiApiService apiService;
    private final StaticResourceHandler staticResources;

    public UiHandler() {
        this(new UiApiService(), UiHandler.class.getClassLoader());
    }

    UiHandler(MetadataAccess metadataAccess) {
        this(new UiApiService(metadataAccess), UiHandler.class.getClassLoader());
    }

    UiHandler(MetadataAccess metadataAccess, ClassLoader classLoader) {
        this(new UiApiService(metadataAccess), classLoader);
    }

    UiHandler(UiApiService apiService, ClassLoader classLoader) {
        this.apiService = Objects.requireNonNull(apiService, "apiService");
        this.staticResources = new StaticResourceHandler(classLoader);
    }

    public FullHttpResponse handleRequest(String uri) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        return handleRequest(decoder.rawPath(), decoder.parameters());
    }

    FullHttpResponse handleRequest(String path, Map<String, List<String>> queryParameters) throws Exception {
        try {
            if (path.startsWith(HttpServerHandler.UI_STATIC_PREFIX)) {
                return staticResources.handle(path);
            }
            if (path.startsWith(HttpServerHandler.UI_API_PREFIX)) {
                return handleApiRequest(path, queryParameters);
            }
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "UI path not found: " + path);
        } catch (IllegalArgumentException e) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST, RestUtils.errorMessage(e, "invalid request"));
        }
    }

    // UI APIs

    private FullHttpResponse handleApiRequest(
            String path, Map<String, List<String>> queryParameters) throws Exception {
        List<String> segments = decodePathSegments(
                path.substring(HttpServerHandler.UI_API_PREFIX.length()));
        if (segments.isEmpty()) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST,
                    "UI API path is empty; expected a resource after " + HttpServerHandler.UI_API_PREFIX);
        }

        return switch (segments.get(0)) {
            case "functions" -> handleFunctions(segments);
            case "functions-dag" -> handleFunctionDag(segments);
            case "tables" -> handleTables(segments);
            case "apis" -> handleApis(segments);
            case "models" -> handleModels(segments, queryParameters);
            case "services" -> handleServices(segments);
            default -> RestUtils.error(HttpResponseStatus.NOT_FOUND,
                    "API not found: " + String.join("/", segments));
        };
    }

    private FullHttpResponse handleFunctions(List<String> segments) {
        if (segments.size() == 1) {
            return ok(apiService.listFunctions());
        }
        if (segments.size() == 2) {
            String name = segments.get(1);
            List<Map<String, String>> detail = apiService.getFunction(name);
            return okOrNotFound(detail, "Function not found: " + name);
        }
        return invalidApiPath(segments);
    }

    private FullHttpResponse handleFunctionDag(List<String> segments) throws Exception {
        if (segments.size() != 2) {
            return invalidApiPath(segments);
        }

        String name = segments.get(1);
        Map<String, Object> dag = apiService.getFunctionDag(name);
        return okOrNotFound(dag, "Function not found: " + name);
    }

    private FullHttpResponse handleTables(List<String> segments) throws Exception {
        if (segments.size() == 2 && "databases".equals(segments.get(1))) {
            return ok(apiService.listDatabases());
        }
        if (segments.size() == 2) {
            return ok(apiService.listTables(segments.get(1)));
        }
        if (segments.size() == 3) {
            String database = segments.get(1);
            String tableName = segments.get(2);
            Map<String, Object> detail = apiService.getTable(database, tableName);
            return okOrNotFound(
                    detail,
                    "Table not found: " + database + "." + tableName
            );
        }
        return invalidApiPath(segments);
    }

    private FullHttpResponse handleApis(List<String> segments) {
        if (segments.size() == 1) {
            return ok(apiService.listApis());
        }
        if (segments.size() == 2) {
            String name = segments.get(1);
            Map<String, Object> detail = apiService.getApi(name);
            return okOrNotFound(detail, "API not found: " + name);
        }
        return invalidApiPath(segments);
    }

    private FullHttpResponse handleModels(
            List<String> segments, Map<String, List<String>> queryParameters) throws Exception {
        if (segments.size() == 1) {
            return ok(apiService.listModels());
        }

        String modelName = segments.get(1);
        if (segments.size() == 2) {
            Map<String, Object> detail = apiService.getModel(modelName);
            return okOrNotFound(detail, "Model not found: " + modelName);
        }
        if (segments.size() == 3 && "checkpoints".equals(segments.get(2))) {
            int page = queryInteger(queryParameters, "page", DEFAULT_PAGE, Integer.MAX_VALUE);
            int pageSize = queryInteger(
                    queryParameters, "pageSize", DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
            return ok(apiService.listCheckpoints(modelName, page, pageSize));
        }
        if (segments.size() == 4 && "checkpoints".equals(segments.get(2))) {
            String checkpointName = segments.get(3);
            Map<String, Object> detail = apiService.getCheckpoint(modelName, checkpointName);
            return okOrNotFound(
                    detail,
                    "Checkpoint not found: " + modelName + "/" + checkpointName
            );
        }
        return invalidApiPath(segments);
    }

    private FullHttpResponse handleServices(List<String> segments) throws Exception {
        if (segments.size() == 1) {
            return ok(apiService.listServices());
        }
        if (segments.size() == 2) {
            String name = segments.get(1);
            Map<String, Object> detail = apiService.getService(name);
            return okOrNotFound(detail, "Service not found: " + name);
        }
        return invalidApiPath(segments);
    }

    // Request helpers

    private List<String> decodePathSegments(String path) {
        try {
            return Arrays.stream(path.split("/", -1))
                    .filter(segment -> !segment.isEmpty())
                    .map(QueryStringDecoder::decodeComponent)
                    .toList();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid API path encoding", e);
        }
    }

    private int queryInteger(Map<String, List<String>> queryParameters,
                             String name, int defaultValue, int maxValue) {
        List<String> values = queryParameters.get(name);
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }

        final int parsed;
        try {
            parsed = Integer.parseInt(values.get(0));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
        if (parsed < 1 || parsed > maxValue) {
            throw new IllegalArgumentException(name + " must be between 1 and " + maxValue);
        }
        return parsed;
    }

    private FullHttpResponse invalidApiPath(List<String> segments) {
        return RestUtils.error(HttpResponseStatus.NOT_FOUND,
                "API not found: " + String.join("/", segments));
    }

    private FullHttpResponse ok(Object result) {
        return RestUtils.ok(JsonUtils.toJson(result));
    }

    private FullHttpResponse okOrNotFound(Object result, String notFoundMessage) {
        return result == null
                ? RestUtils.error(HttpResponseStatus.NOT_FOUND, notFoundMessage)
                : ok(result);
    }

}
