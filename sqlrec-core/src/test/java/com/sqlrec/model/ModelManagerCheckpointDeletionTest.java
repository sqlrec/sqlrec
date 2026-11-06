package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.db.*;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelManagerCheckpointDeletionTest {
    @Test
    void checkpointReferencedByServiceCannotBeDeleted() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1");
            Service service = new Service();
            service.setName("live-service");
            service.setModelName("rank");
            service.setCheckpointName("v1");
            f.store.insertService(service);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.deleteCheckpoint("rank", "v1"));
            assertNotNull(f.store.getCheckpoint("rank", "v1"));
            verifyNoInteractions(f.files);
            f.k8s.verifyNoInteractions();
        }
    }

    @Test
    void deletingIndividualCheckpointsUsesTheirOwnDirectories() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1");
            f.checkpoint("v1/child_export");
            f.checkpoint("v10");
            ModelManager.deleteCheckpoint("rank", "v1/child_export");
            assertNotNull(f.store.getCheckpoint("rank", "v1"));
            ModelManager.deleteCheckpoint("rank", "v1");
            assertNotNull(f.store.getCheckpoint("rank", "v10"));
            verify(f.files).deletePath("/models/rank/v1/child_export");
            verify(f.files).deletePath("/models/rank/v1");
        }
    }

    @Test
    void deletingTheWholeModelStillRemovesAllNestedCheckpoints() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1");
            f.checkpoint("v1/child_export");
            ModelManager.deleteModel("rank");
            assertNull(f.store.getModel("rank"));
            assertTrue(f.store.getCheckpointListByModelName("rank").isEmpty());
            verify(f.files).deletePath("/models/rank");
            verify(f.db, times(1)).getCheckpointListByModelName("rank");
        }
    }

    @Test
    void deletingManyCheckpointsReadsTheModelCheckpointListOnlyOnce() throws Exception {
        try (Fixture f = new Fixture()) {
            for (int i = 0; i < 100; i++) {
                f.checkpoint("v" + i).setStatus(Consts.CHECKPOINT_STATUS_SUCCEEDED);
            }
            ModelManager.deleteModel("rank");
            assertNull(f.store.getModel("rank"));
            assertTrue(f.store.getCheckpointListByModelName("rank").isEmpty());
            verify(f.db, times(1)).getCheckpointListByModelName("rank");
            verify(f.db, times(100)).deleteCheckpoint(eq("rank"), anyString());
        }
    }

    @Test
    void deletingTheWholeModelStillRejectsAReferencedNestedCheckpoint() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoint("v1");
            f.checkpoint("v1/child_export");
            Service service = new Service();
            service.setName("child-service");
            service.setModelName("rank");
            service.setCheckpointName("v1/child_export");
            f.store.insertService(service);
            assertThrows(IllegalArgumentException.class, () -> ModelManager.deleteModel("rank"));
            assertNotNull(f.store.getModel("rank"));
            assertEquals(2, f.store.getCheckpointListByModelName("rank").size());
            verifyNoInteractions(f.files);
            f.k8s.verifyNoInteractions();
        }
    }

    private static class Fixture implements AutoCloseable {
        final InMemoryStoreAccess store = new InMemoryStoreAccess(List.of(), List.of(), List.of());
        final HdfsAccess files = mock(HdfsAccess.class);
        final MetadataAccess db = spy(new MetadataAccess(mock(SchemaAccess.class), store, files));
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);

        Fixture() {
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            Model model = new Model();
            model.setName("rank");
            model.setDdl(ddl("/models/rank"));
            store.insertModel(model);
        }

        String ddl(String path) {
            return "CREATE MODEL `rank` (x FLOAT) WITH ('MODEL_PATH'='" + path + "')";
        }

        Checkpoint checkpoint(String name) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName("rank");
            checkpoint.setCheckpointName(name);
            checkpoint.setModelDdl(ddl("/models/rank"));
            checkpoint.setStatus(Consts.CHECKPOINT_STATUS_FAILED);
            checkpoint.setYaml("old-job");
            store.insertCheckpoint(checkpoint);
            return checkpoint;
        }

        public void close() {
            k8s.close();
            metadata.close();
        }
    }
}
