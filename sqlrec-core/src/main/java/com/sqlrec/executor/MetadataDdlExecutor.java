package com.sqlrec.executor;

import com.sqlrec.db.MetadataAccess;
import com.sqlrec.schema.CacheManager;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlReplaceTableAs;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;

/** Top-level Flink metadata commands; the SQLRec function compiler retains ownership of its body. */
final class MetadataDdlExecutor {
    private MetadataDdlExecutor() {}

    static boolean handles(SqlNode node) {
        // CTAS/RTAS require an execution engine and are deliberately left to the remote route.
        if (node instanceof SqlCreateTableAs || node instanceof SqlReplaceTableAs) {
            return false;
        }
        if (node instanceof SqlCreateTable create) {
            return !create.isTemporary();
        }
        if (node instanceof SqlDropTable drop) {
            return !drop.isTemporary();
        }
        if (node instanceof SqlCreateFunction create) {
            return !create.isTemporary() && !create.isSystemFunction();
        }
        if (node instanceof SqlAlterFunction alter) {
            return !alter.isTemporary() && !alter.isSystemFunction();
        }
        if (node instanceof SqlDropFunction drop) {
            return !drop.isTemporary() && !drop.isSystemFunction();
        }
        return node instanceof SqlAlterTable || node instanceof SqlCreateDatabase
                || node instanceof SqlAlterDatabase || node instanceof SqlDropDatabase;
    }

    static SqlProcessResult execute(MetadataAccess metadata, String sql, String database) throws Exception {
        try {
            metadata.executeMetadataDdl(sql, database);
        } finally {
            // A failed DDL may have changed metadata partially or lost its response after committing.
            CacheManager.invalidateAll();
        }
        return SqlProcessResult.msg("metadata DDL completed", "msg");
    }
}
