package com.sqlrec.db.remote;

import com.sqlrec.db.HdfsAccess;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RemoteHdfsAccess implements HdfsAccess {
    private static final Logger log = LoggerFactory.getLogger(RemoteHdfsAccess.class);
    private final Configuration configuration;

    public RemoteHdfsAccess() {
        this(new Configuration());
    }

    RemoteHdfsAccess(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean pathExists(String hdfsPath) {
        Path path = parsePath(hdfsPath);
        try {
            return getFileSystem(path).exists(path);
        } catch (IOException e) {
            log.error("Check hdfs path exists failed: path={}", hdfsPath, e);
            throw new RuntimeException("Check hdfs path exists failed: path=" + hdfsPath, e);
        }
    }

    @Override
    public void deletePath(String hdfsPath) {
        Path path = parsePath(hdfsPath);
        try {
            FileSystem filesystem = getFileSystem(path);
            if (filesystem.exists(path) && !moveToTrash(filesystem, path) && !filesystem.delete(path, true)) {
                throw new IOException("FileSystem.delete returned false: " + path);
            }
        } catch (IOException e) {
            log.error("Delete hdfs path failed: path={}", hdfsPath, e);
            throw new RuntimeException("Delete hdfs path failed: path=" + hdfsPath, e);
        }
        log.info("Delete hdfs path success: path={}", hdfsPath);
    }

    private static Path parsePath(String hdfsPath) {
        if (StringUtils.isBlank(hdfsPath)) {
            throw new IllegalArgumentException("hdfsPath cannot be blank");
        }
        return new Path(hdfsPath);
    }

    FileSystem getFileSystem(Path path) throws IOException {
        return path.getFileSystem(configuration);
    }

    boolean moveToTrash(FileSystem filesystem, Path path) throws IOException {
        return Trash.moveToAppropriateTrash(filesystem, path, configuration);
    }
}
