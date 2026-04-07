package com.sqlrec.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

public class HadoopUtils {
    private static final Logger log = LoggerFactory.getLogger(HadoopUtils.class);

    public static boolean pathExists(String hdfsPath) {
        if (hdfsPath == null || StringUtils.containsWhitespace(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be blank");
        }

        try {
            ProcessBuilder pb = new ProcessBuilder("hadoop", "fs", "-test", "-e", hdfsPath);
            pb.redirectErrorStream(true);
            clearJavaToolOptions(pb.environment());
            Process process = pb.start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            throw new RuntimeException("Check hdfs path exists failed: path=" + hdfsPath, e);
        }
    }

    public static void deletePath(String hdfsPath) {
        if (StringUtils.isEmpty(hdfsPath) || StringUtils.containsWhitespace(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be blank");
        }

        try {
            ProcessBuilder pb = new ProcessBuilder("hadoop", "fs", "-rm", "-r", "-f", hdfsPath);
            pb.redirectErrorStream(true);
            clearJavaToolOptions(pb.environment());
            Process process = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                log.warn("Delete hdfs path failed: path={}, exitCode={}, output={}", hdfsPath, exitCode, output);
                throw new RuntimeException("Delete hdfs path failed: path=" + hdfsPath + " output=" + output);
            }
            log.info("Delete hdfs path success: path={}", hdfsPath);
        } catch (Exception e) {
            throw new RuntimeException("Delete hdfs path failed: path=" + hdfsPath, e);
        }
    }

    private static void clearJavaToolOptions(Map<String, String> environment) {
        environment.remove("JAVA_TOOL_OPTIONS");
    }
}
