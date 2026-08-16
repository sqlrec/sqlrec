package com.sqlrec.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

public class PathUtils {

    public static void validateModelPath(String hdfsPath, String modelPath) {
        if (StringUtils.isBlank(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be blank");
        }
        if (StringUtils.isBlank(modelPath)) {
            throw new IllegalArgumentException("modelPath cannot be blank");
        }

        // When both paths carry an explicit authority (e.g. hdfs://nn1 vs hdfs://nn2),
        // they must match so a path cannot sneak onto a different cluster.
        String pathAuthority = extractAuthority(hdfsPath);
        String modelPathAuthority = extractAuthority(modelPath);
        if (pathAuthority != null && modelPathAuthority != null && !pathAuthority.equals(modelPathAuthority)) {
            throw new IllegalArgumentException(
                    "Path authority must match model path authority. Path: " + hdfsPath + ", Model path: " + modelPath);
        }

        String normalizedPath = normalizePath(hdfsPath);
        String normalizedModelPath = normalizePath(modelPath);

        if (!normalizedPath.equals(normalizedModelPath)
                && !normalizedPath.startsWith(normalizedModelPath + "/")) {
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

        // FilenameUtils.normalize resolves "." and ".." segments. unixSeparator=true forces
        // "/" as the output separator regardless of the platform (the one-argument overload
        // converts to the system separator, e.g. "\" on Windows). It returns null when the
        // path would escape above the root (or is otherwise invalid): treat that as a
        // collapsed root, so the prefix check in validateModelPath rejects it.
        String result = FilenameUtils.normalize(normalized, true);
        while (result != null && result.endsWith("/") && result.length() > 1) {
            result = result.substring(0, result.length() - 1);
        }
        if (result == null || result.equals("/")) {
            return "";
        }
        return result;
    }

    /**
     * Returns the authority of a "scheme://authority/path" style path
     * (e.g. "namenode:8020"), or null when the path has no scheme or no authority.
     */
    static String extractAuthority(String path) {
        if (path == null) {
            return null;
        }
        int protocolIndex = path.indexOf("://");
        if (protocolIndex == -1) {
            return null;
        }
        String rest = path.substring(protocolIndex + 3);
        int slashIndex = rest.indexOf("/");
        String authority = slashIndex == -1 ? rest : rest.substring(0, slashIndex);
        return authority.isEmpty() ? null : authority;
    }
}
