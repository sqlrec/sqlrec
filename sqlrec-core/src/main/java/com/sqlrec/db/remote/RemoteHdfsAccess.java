package com.sqlrec.db.remote;

import com.sqlrec.db.HdfsAccess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RemoteHdfsAccess implements HdfsAccess {
    private static final Logger log = LoggerFactory.getLogger(RemoteHdfsAccess.class);
    private static final long DEFAULT_TIMEOUT_SECONDS = 300;

    @Override
    public boolean pathExists(String hdfsPath) {
        validatePath(hdfsPath);

        try {
            Process process = startProcess("hadoop", "fs", "-test", "-e", hdfsPath);
            boolean finished = process.waitFor(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.error("Check hdfs path exists timeout: path={}", hdfsPath);
                throw new RuntimeException("Check hdfs path exists timeout: path=" + hdfsPath);
            }
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                log.warn("Check hdfs path exists failed: path={}, exitCode={}", hdfsPath, exitCode);
            }
            return exitCode == 0;
        } catch (Exception e) {
            log.error("Check hdfs path exists exception: path={}", hdfsPath, e);
            throw new RuntimeException("Check hdfs path exists failed: path=" + hdfsPath, e);
        }
    }

    @Override
    public void deletePath(String hdfsPath) {
        validatePath(hdfsPath);

        try {
            Process process = startProcess("hadoop", "fs", "-rm", "-r", "-f", hdfsPath);
            boolean finished = process.waitFor(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new RuntimeException("Delete hdfs path timeout: path=" + hdfsPath);
            }
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                log.warn("Delete hdfs path failed: path={}, exitCode={}", hdfsPath, exitCode);
                throw new RuntimeException("Delete hdfs path failed: path=" + hdfsPath);
            }
            log.info("Delete hdfs path success: path={}", hdfsPath);
        } catch (Exception e) {
            throw new RuntimeException("Delete hdfs path failed: path=" + hdfsPath, e);
        }
    }

    private void validatePath(String hdfsPath) {
        if (hdfsPath == null || StringUtils.containsWhitespace(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be blank");
        }

        if (hdfsPath.contains(";") || hdfsPath.contains("|") || hdfsPath.contains("&") ||
                hdfsPath.contains("`") || hdfsPath.contains("$") || hdfsPath.contains("(") ||
                hdfsPath.contains(")") || hdfsPath.contains("<") || hdfsPath.contains(">")) {
            throw new IllegalArgumentException("hdfsPath contains invalid characters: " + hdfsPath);
        }
    }

    private Process startProcess(String... command) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        // Avoid a PIPE reader blocking before the timed wait can run.
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        return builder.start();
    }
}
