package com.sqlrec.frontend.rest;

import com.sqlrec.frontend.utils.RestUtils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Classpath resources, their cache policy, and SPA fallback for the UI. */
final class StaticResourceHandler {
    private static final String STATIC_ROOT = "ui/static/";
    private final ClassLoader classLoader;
    private final Map<String, StaticResource> staticResourceCache = new ConcurrentHashMap<>();

    StaticResourceHandler(ClassLoader classLoader) {
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    }

    FullHttpResponse handle(String path) throws Exception {
        String resourcePath = decodeResourcePath(
                path.substring(HttpServerHandler.UI_STATIC_PREFIX.length()));
        if (isForbiddenResourcePath(resourcePath)) {
            return RestUtils.error(HttpResponseStatus.FORBIDDEN,
                    "static resource path is forbidden: " + resourcePath);
        }

        String requestedPath = resourcePath.isEmpty() ? "index.html" : resourcePath;
        StaticResource resource = loadStaticResource(requestedPath);
        if (resource == null && isSpaRoute(requestedPath)) {
            resource = loadStaticResource("index.html");
        }
        if (resource == null) {
            return RestUtils.error(HttpResponseStatus.NOT_FOUND,
                    "static resource not found: " + requestedPath);
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
