package com.sqlrec.db.remote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises both deployed storage backends, independently of fs.defaultFS. */
@Tag("integration")
@Timeout(120)
class RemoteHdfsAccessIntegrationTest {
    private FileSystem filesystem;
    private RemoteHdfsAccess access;
    private Path directory;

    void setUp(String uri, String expectedScheme) throws Exception {
        Configuration configuration = new Configuration();
        filesystem = FileSystem.get(URI.create(uri), configuration);
        assertEquals(expectedScheme, filesystem.getUri().getScheme());
        if ("hdfs".equals(expectedScheme)) {
            assertInstanceOf(DistributedFileSystem.class, filesystem);
        }
        String basePath = System.getenv().getOrDefault("HADOOP_TEST_BASE_PATH", "/tmp");
        directory = filesystem.makeQualified(new Path(basePath, "sqlrec-test-" + UUID.randomUUID()));
        assertEquals(expectedScheme, directory.toUri().getScheme());
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
    void juicefsCreatesReadsRenamesAndRecursivelyDeletesDirectories() throws Exception {
        setUp(System.getenv().getOrDefault("HADOOP_TEST_JUICEFS_URI", "jfs://myjfs"), "jfs");
        createsReadsRenamesAndRecursivelyDeletesDirectories();
    }

    @Test
    void hdfsCreatesReadsRenamesAndRecursivelyDeletesDirectories() throws Exception {
        setUp(System.getenv().getOrDefault("HADOOP_TEST_HDFS_URI", "hdfs://myhdfs"), "hdfs");
        createsReadsRenamesAndRecursivelyDeletesDirectories();
    }

    private void createsReadsRenamesAndRecursivelyDeletesDirectories() throws Exception {
        Path nested = new Path(directory, "nested/checkpoint");
        assertTrue(filesystem.mkdirs(nested));
        Path model = new Path(nested, "model.txt");
        byte[] content = "sqlrec model data".getBytes(StandardCharsets.UTF_8);
        try (var output = filesystem.create(model, false)) {
            output.write(content);
        }
        // TensorBoard repeatedly opens event files for append. Exercise that
        // path with the same replication requested by PyArrow's default client.
        if (filesystem instanceof DistributedFileSystem) {
            Path events = new Path(nested, "events");
            try (var output = filesystem.create(events, true, 4096, (short) 3, 128 * 1024 * 1024)) {
                output.write(content);
            }
            try (var output = filesystem.append(events)) {
                output.write(content);
            }
            try (var input = filesystem.open(events)) {
                assertArrayEquals("sqlrec model data".repeat(2).getBytes(StandardCharsets.UTF_8),
                        input.readAllBytes());
            }
            assertTrue(filesystem.delete(events, false));
        }
        try (var input = filesystem.open(model)) {
            assertArrayEquals(content, input.readAllBytes());
        }
        assertEquals(model, filesystem.listStatus(nested)[0].getPath());
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
        setUp(System.getenv().getOrDefault("HADOOP_TEST_JUICEFS_URI", "jfs://myjfs"), "jfs");
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
