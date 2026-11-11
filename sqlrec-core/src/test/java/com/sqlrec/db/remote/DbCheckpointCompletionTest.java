package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.Checkpoint;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DbCheckpointCompletionTest {
    @Test
    void completionIsCommittedAndDoesNotChangeOtherCheckpointFields() throws Exception {
        try (Fixture f = new Fixture()) {
            Checkpoint expected = f.db.getCheckpoint("rank", "v1");
            assertTrue(f.db.completeCheckpoint(expected, Consts.CHECKPOINT_STATUS_SUCCEEDED));
            Checkpoint saved = f.db.getCheckpoint("rank", "v1");
            assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, saved.getStatus());
            assertEquals("old-yaml", saved.getYaml());
            assertEquals("model-ddl", saved.getModelDdl());
            assertTrue(saved.getUpdatedAt() > 1);
            assertFalse(f.db.completeCheckpoint(saved, Consts.CHECKPOINT_STATUS_FAILED));
        }
    }

    @Test
    void aDeletedCheckpointIsNeverReinserted() throws Exception {
        try (Fixture f = new Fixture()) {
            Checkpoint expected = f.db.getCheckpoint("rank", "v1");
            f.execute("DELETE FROM checkpoint");
            assertFalse(f.db.completeCheckpoint(expected, Consts.CHECKPOINT_STATUS_SUCCEEDED));
            assertNull(f.db.getCheckpoint("rank", "v1"));
        }
    }

    @Test
    void changedVersionOrYamlPreventsOldCompletion() throws Exception {
        for (String change : new String[]{"updated_at = 2", "yaml = 'new-yaml'"}) {
            try (Fixture f = new Fixture()) {
                Checkpoint expected = f.db.getCheckpoint("rank", "v1");
                f.execute("UPDATE checkpoint SET " + change);
                assertFalse(f.db.completeCheckpoint(expected, Consts.CHECKPOINT_STATUS_SUCCEEDED));
                assertEquals(Consts.CHECKPOINT_STATUS_CREATED, f.db.getCheckpoint("rank", "v1").getStatus());
            }
        }
    }

    @Test
    void nullYamlCanStillBeMarkedFailed() throws Exception {
        try (Fixture f = new Fixture()) {
            f.execute("UPDATE checkpoint SET yaml = NULL");
            assertTrue(f.db.completeCheckpoint(f.db.getCheckpoint("rank", "v1"), Consts.CHECKPOINT_STATUS_FAILED));
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, f.db.getCheckpoint("rank", "v1").getStatus());
        }
    }

    private static final class Fixture implements AutoCloseable {
        final Connection connection;
        final MetadataAccess db;

        Fixture() throws Exception {
            var source = new UnpooledDataSource("org.h2.Driver",
                    "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL", "sa", "");
            connection = source.getConnection();
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE checkpoint (model_name VARCHAR, checkpoint_name VARCHAR, "
                        + "model_ddl VARCHAR, yaml VARCHAR, status VARCHAR, updated_at BIGINT, "
                        + "PRIMARY KEY (model_name, checkpoint_name))");
                statement.execute("INSERT INTO checkpoint VALUES ('rank', 'v1', 'model-ddl', 'old-yaml', 'created', 1)");
            }
            var configuration = new Configuration(new Environment("test", new JdbcTransactionFactory(), source));
            configuration.setMapUnderscoreToCamelCase(true);
            configuration.addMapper(DbMapper.class);
            SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(configuration);
            var store = new DbStoreAccess() {
                @Override public SqlSessionFactory getSqlSessionFactory() { return factory; }
            };
            db = new MetadataAccess(null, store, null);
        }

        @Override public void close() throws Exception { connection.close(); }

        void execute(String sql) throws Exception {
            try (var statement = connection.createStatement()) { statement.executeUpdate(sql); }
        }
    }
}
