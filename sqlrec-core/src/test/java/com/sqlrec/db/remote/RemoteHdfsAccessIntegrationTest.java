package com.sqlrec.db.remote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Uses the deployed Hadoop configuration and filesystem, including JuiceFS. */
@Tag("integration")
@Timeout(120)
class RemoteHdfsAccessIntegrationTest {
    private FileSystem filesystem;
    private RemoteHdfsAccess access;
    private Path directory;

    @BeforeEach
    void setUp() throws Exception {
        Configuration configuration = new Configuration();
        assertNotEquals("file", FileSystem.getDefaultUri(configuration).getScheme(),
                "Configure a remote fs.defaultFS through HADOOP_CONF_DIR and bin/test.sh");
        filesystem = FileSystem.get(configuration);
        String basePath = System.getenv().getOrDefault("HADOOP_TEST_BASE_PATH", "/tmp");
        directory = filesystem.makeQualified(new Path(basePath, "sqlrec-test-" + UUID.randomUUID()));
        access = new RemoteHdfsAccess();
        assertFalse(access.pathExists(directory.toString()));
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (filesystem != null && directory != null) {
            // Only remove the unique directory created by this test. Do not close
            // the cached FileSystem shared with RemoteHdfsAccess and other tests.
            filesystem.delete(directory, true);
        }
    }

    @Test
    void createsReadsRenamesAndRecursivelyDeletesDirectories() throws Exception {
        Path nested = new Path(directory, "nested/checkpoint");
        assertTrue(filesystem.mkdirs(nested));
        Path model = new Path(nested, "model.txt");
        byte[] content = "sqlrec model data".getBytes(StandardCharsets.UTF_8);
        try (var output = filesystem.create(model, false)) {
            output.write(content);
        }
        try (var input = filesystem.open(model)) {
            assertArrayEquals(content, input.readAllBytes());
        }
        assertTrue(access.pathExists(directory.toString()));
        assertTrue(access.pathExists(model.toString()));

        Path renamed = new Path(directory, "renamed");
        assertTrue(filesystem.rename(new Path(directory, "nested"), renamed));
        assertFalse(access.pathExists(model.toString()));
        assertTrue(access.pathExists(new Path(renamed, "checkpoint/model.txt").toString()));

        access.deletePath(directory.toString());
        assertFalse(filesystem.exists(directory));
        assertFalse(access.pathExists(directory.toString()));
        assertDoesNotThrow(() -> access.deletePath(directory.toString()));
    }

    @Test
    @EnabledIfSystemProperty(named = "sqlrec.test.hadoop.groupRefresh", matches = "true")
    void cachedClientCanStillDeleteAfterGroupMappingRefresh() throws Exception {
        assertTrue(filesystem.mkdirs(new Path(directory, "before-refresh/checkpoint")));
        assertTrue(access.pathExists(directory.toString()));

        // JuiceFS refreshes configured user/group mappings once a minute.
        // Keep the same client alive across that refresh, as the server does.
        Thread.sleep(Duration.ofSeconds(65));

        assertTrue(filesystem.mkdirs(new Path(directory, "after-refresh/checkpoint")));
        access.deletePath(directory.toString());
        assertFalse(filesystem.exists(directory));
    }
}
