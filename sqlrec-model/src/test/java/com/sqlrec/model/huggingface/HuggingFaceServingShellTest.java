package com.sqlrec.model.huggingface;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HuggingFaceServingShellTest {
    @TempDir
    Path directory;

    @Test
    void passesCheckpointPathWithoutInterpretingShellCharacters() throws Exception {
        Path bin = Files.createDirectory(directory.resolve("bin"));
        Path localModel = directory.resolve("local model");
        Path capturedDownloadArgs = directory.resolve("download-args");
        Path capturedServerArgs = directory.resolve("server-args");
        // Replace external programs so the generated startup command runs without a model or cluster.
        executable(bin.resolve("hadoop"), "touch \"$LOCAL_MODEL_DIR/_SUCCESS\"\n"
                + "printf '%s\\n' \"$@\" > \"$CAPTURE_DOWNLOAD_ARGS\"\n");
        executable(bin.resolve("python"), "printf '%s\\n' \"$@\" > \"$CAPTURE_SERVER_ARGS\"\n");
        String checkpoint = "hdfs://namenode/models/quoted' $(touch INJECTED) `touch INJECTED2`";
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", ShellUtils.genServeShell(checkpoint))
                .directory(directory.toFile()).redirectErrorStream(true);
        builder.environment().putAll(Map.of(
                "PATH", bin + ":" + System.getenv("PATH"),
                "HADOOP_HOME", directory.toString(),
                "LOCAL_MODEL_DIR", localModel.toString(),
                "CAPTURE_DOWNLOAD_ARGS", capturedDownloadArgs.toString(),
                "CAPTURE_SERVER_ARGS", capturedServerArgs.toString()));
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Serving shell timed out");
            assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes()));
            assertEquals(List.of("fs", "-get", checkpoint + "/*", localModel + "/"),
                    Files.readAllLines(capturedDownloadArgs));
            assertEquals(List.of("-m", "huggingface.server", "--model_dir", localModel.toString(),
                    "--config", "/data/service.config", "--port", "80"),
                    Files.readAllLines(capturedServerArgs));
            assertFalse(Files.exists(directory.resolve("INJECTED")));
            assertFalse(Files.exists(directory.resolve("INJECTED2")));
        } finally {
            process.destroyForcibly();
        }
    }

    private void executable(Path path, String content) throws Exception {
        Files.writeString(path, "#!/bin/bash\nset -e\n" + content);
        assertTrue(path.toFile().setExecutable(true));
    }
}
