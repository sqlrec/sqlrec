package com.sqlrec.db;

public interface HdfsAccess {

    boolean pathExists(String hdfsPath);

    void deletePath(String hdfsPath);
}
