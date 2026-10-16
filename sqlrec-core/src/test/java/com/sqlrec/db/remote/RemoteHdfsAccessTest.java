package com.sqlrec.db.remote;

import com.sqlrec.common.utils.SilenceLoggers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RemoteHdfsAccessTest {
    @TempDir
    java.nio.file.Path temporary;

    @Test
    @SilenceLoggers(RemoteHdfsAccess.class)
    void onlyMissingPathsReturnFalseWhileStorageFailuresPropagate() throws Exception {
        Configuration configuration = new Configuration(false);
        Path path = new Path("hdfs://namenode/models/checkpoint");
        FileSystem filesystem = mock(FileSystem.class, CALLS_REAL_METHODS);
        RemoteHdfsAccess access = spy(new RemoteHdfsAccess(configuration));
        doReturn(filesystem).when(access).getFileSystem(path);
        doReturn(new FileStatus()).when(filesystem).getFileStatus(path);
        assertTrue(access.pathExists(path.toString()));
        doThrow(new FileNotFoundException("missing")).when(filesystem).getFileStatus(path);
        assertFalse(access.pathExists(path.toString()));
        for (IOException failure : new IOException[]{
                new AccessControlException("denied"), new IOException("connection failed")}) {
            doThrow(failure).when(filesystem).getFileStatus(path);
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> access.pathExists(path.toString()));
            assertSame(failure, error.getCause());
        }
    }

    @Test
    void javaClientDeletesRecursivelyAndAllowsLiteralShellCharacters() throws Exception {
        java.nio.file.Path directory = temporary.resolve("model with spaces;$()&*");
        Files.createDirectories(directory.resolve("nested"));
        Files.writeString(directory.resolve("nested/checkpoint"), "model data");
        Configuration configuration = new Configuration(false);
        configuration.set("fs.defaultFS", "file:///");
        configuration.setLong("fs.trash.interval", 0);
        RemoteHdfsAccess access = new RemoteHdfsAccess(configuration);
        assertTrue(access.pathExists(directory.toString()));
        access.deletePath(directory.toString());
        assertFalse(Files.exists(directory));
        access.deletePath(directory.toString());
        assertFalse(access.pathExists(directory.toString()));
    }

    @Test
    void missingPathsSkipTrashAndDeletion() throws Exception {
        Path path = new Path("hdfs://namenode/missing");
        FileSystem filesystem = mock(FileSystem.class);
        RemoteHdfsAccess access = spy(new RemoteHdfsAccess(new Configuration(false)));
        doReturn(filesystem).when(access).getFileSystem(path);
        access.deletePath(path.toString());
        verify(access, never()).moveToTrash(any(), any());
        verify(filesystem, never()).delete(any(), anyBoolean());
    }

    @Test
    @SilenceLoggers(RemoteHdfsAccess.class)
    void deletionFailuresAreReportedInsteadOfReturningSuccess() throws Exception {
        Path path = new Path("hdfs://namenode/models/checkpoint");
        FileSystem filesystem = mock(FileSystem.class);
        doReturn(true).when(filesystem).exists(path);
        RemoteHdfsAccess access = spy(new RemoteHdfsAccess(new Configuration(false)));
        doReturn(filesystem).when(access).getFileSystem(path);
        doReturn(false).when(access).moveToTrash(filesystem, path);
        RuntimeException rejected = assertThrows(RuntimeException.class, () -> access.deletePath(path.toString()));
        assertInstanceOf(IOException.class, rejected.getCause());
        assertTrue(rejected.getCause().getMessage().contains("returned false"));
        IOException failure = new AccessControlException("denied");
        doThrow(failure).when(filesystem).delete(path, true);
        RuntimeException error = assertThrows(RuntimeException.class, () -> access.deletePath(path.toString()));
        assertSame(failure, error.getCause());
        doThrow(failure).when(filesystem).exists(path);
        error = assertThrows(RuntimeException.class, () -> access.deletePath(path.toString()));
        assertSame(failure, error.getCause());
    }

    @Test
    @SilenceLoggers(RemoteHdfsAccess.class)
    void trashSuccessOrFailureNeverFallsThroughToPermanentDeletion() throws Exception {
        Path path = new Path("hdfs://namenode/models/checkpoint");
        FileSystem filesystem = mock(FileSystem.class);
        doReturn(true).when(filesystem).exists(path);
        RemoteHdfsAccess access = spy(new RemoteHdfsAccess(new Configuration(false)));
        doReturn(filesystem).when(access).getFileSystem(path);
        doReturn(true).when(access).moveToTrash(filesystem, path);
        access.deletePath(path.toString());
        IOException failure = new IOException("trash unavailable");
        doThrow(failure).when(access).moveToTrash(filesystem, path);
        RuntimeException error = assertThrows(RuntimeException.class, () -> access.deletePath(path.toString()));
        assertSame(failure, error.getCause());
        verify(filesystem, never()).delete(any(), anyBoolean());
    }

    @Test
    void defaultClientLoadsCoreAndHdfsSettingsFromClasspath() throws Exception {
        Files.writeString(temporary.resolve("core-site.xml"), """
                <configuration><property><name>fs.defaultFS</name>
                <value>hdfs://configured-namenode:8020</value></property></configuration>
                """);
        Files.writeString(temporary.resolve("hdfs-site.xml"), """
                <configuration><property><name>dfs.replication</name>
                <value>2</value></property></configuration>
                """);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        HdfsConfiguration.init();
        try (URLClassLoader loader = new URLClassLoader(new URL[]{temporary.toUri().toURL()}, original) {
            @Override
            public URL getResource(String name) {
                // Isolate this fixture from deployed configuration added by bin/test.sh.
                if ("core-site.xml".equals(name) || "hdfs-site.xml".equals(name)) {
                    return findResource(name);
                }
                return super.getResource(name);
            }
        };
             var factory = mockStatic(FileSystem.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            Path path = new Path("hdfs://configured-namenode:8020/models/checkpoint");
            FileSystem filesystem = mock(FileSystem.class);
            factory.when(() -> FileSystem.get(eq(path.toUri()), any(Configuration.class))).thenAnswer(invocation -> {
                Configuration configuration = invocation.getArgument(1);
                assertEquals("hdfs://configured-namenode:8020", configuration.get("fs.defaultFS"));
                assertEquals(2, configuration.getInt("dfs.replication", 0));
                return filesystem;
            });
            assertSame(filesystem, new RemoteHdfsAccess().getFileSystem(path));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void blankPathsAreRejectedBeforeOpeningAFileSystem() {
        RemoteHdfsAccess access = new RemoteHdfsAccess(new Configuration(false));
        for (String path : new String[]{null, "", " ", "\t"}) {
            assertThrows(IllegalArgumentException.class, () -> access.pathExists(path));
            assertThrows(IllegalArgumentException.class, () -> access.deletePath(path));
        }
    }
}
