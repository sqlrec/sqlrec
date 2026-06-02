package com.sqlrec.utils;

public class PathUtils {

    public static void validateModelPath(String hdfsPath, String modelPath) {
        String normalizedPath = normalizePath(hdfsPath);
        String normalizedModelPath = normalizePath(modelPath);

        if (!normalizedPath.startsWith(normalizedModelPath)) {
            throw new IllegalArgumentException("Path must be under model path. Path: " + hdfsPath + ", Model path: " + modelPath);
        }
    }

    public static String normalizePath(String path) {
        if (path == null) {
            return "";
        }

        String normalized = path;

        int protocolIndex = normalized.indexOf("://");
        if (protocolIndex != -1) {
            normalized = normalized.substring(protocolIndex + 3);
            int slashIndex = normalized.indexOf("/");
            if (slashIndex != -1) {
                normalized = normalized.substring(slashIndex);
            }
        }

        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }

        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        return normalized;
    }
}
