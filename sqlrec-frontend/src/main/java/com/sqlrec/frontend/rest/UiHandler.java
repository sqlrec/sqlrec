package com.sqlrec.frontend.rest;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.frontend.utils.RestUtils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class UiHandler {
    private static final String STATIC_ROOT = "ui/static/";
    private static final int DEFAULT_PAGE = 1;
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;

    private final UiApiService apiService;
    private final ClassLoader classLoader;
    private final Map<String, StaticResource> staticResourceCache = new ConcurrentHashMap<>();

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
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    }

    public FullHttpResponse handleRequest(String uri) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        return handleRequest(decoder.rawPath(), decoder.parameters());
    }

    FullHttpResponse handleRequest(String path, Map<String, List<String>> queryParameters) throws Exception {
        try {
            if (path.startsWith(HttpServerHandler.UI_STATIC_PREFIX)) {
                return handleStaticResource(path);
            }
            if (path.startsWith(HttpServerHandler.UI_API_PREFIX)) {
                return handleApiRequest(path, queryParameters);
            }
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "UI path not found");
        } catch (IllegalArgumentException e) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST, errorMessage(e, "invalid request"));
        }
    }

    // Static resources

    private FullHttpResponse handleStaticResource(String path) throws Exception {
        String resourcePath = decodeResourcePath(
                path.substring(HttpServerHandler.UI_STATIC_PREFIX.length()));
        if (isForbiddenResourcePath(resourcePath)) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN, "Forbidden resource path");
        }

        String requestedPath = resourcePath.isEmpty() ? "index.html" : resourcePath;
        StaticResource resource = loadStaticResource(requestedPath);
        if (resource == null && isSpaRoute(requestedPath)) {
            resource = loadStaticResource("index.html");
        }
        if (resource == null) {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND, "Static resource not found");
        }

        return RestUtils.ok(
                Arrays.copyOf(resource.content(), resource.content().length),
                resource.contentType(),
                Map.of("Cache-Control", resource.cacheControl()));
    }

    private StaticResource loadStaticResource(String resourcePath) throws Exception {
        StaticResource cached = staticResourceCache.get(resourcePath);
        if (cached != null) {
            return cached;
        }

        try (InputStream input = classLoader.getResourceAsStream(STATIC_ROOT + resourcePath)) {
            if (input == null) {
                return null;
            }
            StaticResource loaded = new StaticResource(
                    input.readAllBytes(),
                    getContentType(resourcePath),
                    getCacheControl(resourcePath));
            staticResourceCache.put(resourcePath, loaded);
            return loaded;
        }
    }

    private String decodeResourcePath(String resourcePath) {
        try {
            return QueryStringDecoder.decodeComponent(resourcePath);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid resource path encoding", e);
        }
    }

    private boolean isForbiddenResourcePath(String resourcePath) {
        if (resourcePath.startsWith("/") || resourcePath.startsWith("\\")
                || resourcePath.indexOf('\0') >= 0 || resourcePath.contains("\\")) {
            return true;
        }
        return Arrays.stream(resourcePath.split("/", -1))
                .anyMatch(segment -> segment.equals(".") || segment.equals(".."));
    }

    private boolean isSpaRoute(String resourcePath) {
        int slashIndex = resourcePath.lastIndexOf('/');
        String fileName = slashIndex >= 0 ? resourcePath.substring(slashIndex + 1) : resourcePath;
        return !fileName.contains(".");
    }

    // UI APIs

    private FullHttpResponse handleApiRequest(
            String path, Map<String, List<String>> queryParameters) throws Exception {
        List<String> segments = decodePathSegments(
                path.substring(HttpServerHandler.UI_API_PREFIX.length()));
        if (segments.isEmpty()) {
            return RestUtils.error(HttpResponseStatus.BAD_REQUEST, "API path is empty");
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND, "Function not found: " + name)
                    : ok(detail);
        }
        return invalidApiPath(segments);
    }

    private FullHttpResponse handleFunctionDag(List<String> segments) throws Exception {
        if (segments.size() != 2) {
            return invalidApiPath(segments);
        }

        String name = segments.get(1);
        Map<String, Object> dag = apiService.getFunctionDag(name);
        return dag == null
                ? RestUtils.error(HttpResponseStatus.NOT_FOUND, "Function not found: " + name)
                : ok(dag);
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND,
                    "Table not found: " + database + "." + tableName)
                    : ok(detail);
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND, "API not found: " + name)
                    : ok(detail);
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND, "Model not found: " + modelName)
                    : ok(detail);
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND,
                    "Checkpoint not found: " + modelName + "/" + checkpointName)
                    : ok(detail);
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
            return detail == null
                    ? RestUtils.error(HttpResponseStatus.NOT_FOUND, "Service not found: " + name)
                    : ok(detail);
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

    private String errorMessage(Exception exception, String fallback) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? fallback : message;
    }

    // Static resource metadata

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
        }
        return "application/octet-stream";
    }

    private static String getCacheControl(String path) {
        if ("index.html".equals(path)) {
            return "no-cache";
        }
        if (path.matches(".*-[A-Za-z0-9_-]{8,}\\.[^.]+$")) {
            return "public, max-age=31536000, immutable";
        }
        return "public, max-age=86400";
    }

    private record StaticResource(byte[] content, String contentType, String cacheControl) {
    }
}
