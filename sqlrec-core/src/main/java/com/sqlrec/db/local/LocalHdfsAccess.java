package com.sqlrec.db.local;

import com.sqlrec.db.HdfsAccess;

public class LocalHdfsAccess implements HdfsAccess {

    @Override
    public boolean pathExists(String hdfsPath) {
        throw new UnsupportedOperationException("HDFS access is not supported in local mode");
    }

    @Override
    public void deletePath(String hdfsPath) {
        throw new UnsupportedOperationException("HDFS access is not supported in local mode");
    }
}
