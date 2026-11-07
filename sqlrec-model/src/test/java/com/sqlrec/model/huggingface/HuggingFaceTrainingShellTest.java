package com.sqlrec.model.huggingface;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HuggingFaceTrainingShellTest {
    @TempDir
    Path directory;

    @Test
    void publishesACompleteSnapshotIntoAnAbsentTarget() throws Exception {
        Path target = directory.resolve("models base/v1");

        ShellResult result = train(target);

        assertEquals(0, result.exitCode(), result.output());
        assertEquals("new", Files.readString(target.resolve("config.json")).strip());
        assertTrue(Files.exists(target.resolve("_SUCCESS")));
        assertFalse(Files.exists(Path.of(target + ".__uploading__.download-job")));
    }

    @Test
    void existingCompleteOrIncompleteTargetFailsWithoutChangingItsContents() throws Exception {
        for (boolean complete : new boolean[]{false, true}) {
            Path target = Files.createDirectories(directory.resolve("models base/" + complete));
            Files.writeString(target.resolve("config.json"), "old");
            if (complete) Files.writeString(target.resolve("_SUCCESS"), "");

            ShellResult result = train(target);

            assertNotEquals(0, result.exitCode(), result.output());
            assertTrue(result.output().contains("Checkpoint directory already exists: " + target));
            assertEquals("old", Files.readString(target.resolve("config.json")));
            assertEquals(complete, Files.exists(target.resolve("_SUCCESS")));
            assertFalse(Files.exists(target.resolve(target.getFileName() + ".__uploading__.download-job")));
        }
    }

    private ShellResult train(Path target) throws Exception {
        Path bin = Files.createDirectories(directory.resolve("bin"));
        // Run the generated script, replacing Hub access and Hadoop with local filesystem commands.
        executable(bin.resolve("python"), """
                mkdir -p "$LOCAL_MODEL_DIR"
                echo new > "$LOCAL_MODEL_DIR/config.json"
                """);
        executable(bin.resolve("hadoop"), """
                case "$1 $2" in
                    "fs -mkdir") mkdir -p "$4" ;;
                    "fs -rm") rm -rf -- "$5" ;;
                    "fs -put") cp -R "$4" "$5" ;;
                    "fs -touchz") touch "$3" ;;
                    "fs -test") test -e "$4" ;;
                    "fs -mv") mv "$3" "$4" ;;
                    *) echo "Unexpected Hadoop command" >&2; exit 2 ;;
                esac
                """);
        ProcessBuilder builder = new ProcessBuilder("bash", "-c",
                ShellUtils.genTrainShell(target.getParent().toString(), target.toString(), "download-job"))
                .directory(directory.toFile()).redirectErrorStream(true);
        builder.environment().putAll(Map.of(
                "PATH", bin + ":" + System.getenv("PATH"),
                "HADOOP_HOME", directory.toString(),
                "LOCAL_MODEL_DIR", directory.resolve("local model").toString()));
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Training shell timed out");
            return new ShellResult(process.exitValue(),
                    new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            process.destroyForcibly();
        }
    }

    private void executable(Path path, String content) throws Exception {
        Files.writeString(path, "#!/bin/bash\nset -e\n" + content);
        assertTrue(path.toFile().setExecutable(true));
    }

    private record ShellResult(int exitCode, String output) {}
}
