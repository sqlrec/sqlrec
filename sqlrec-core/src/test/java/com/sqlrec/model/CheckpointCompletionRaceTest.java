package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.k8s.K8sManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CheckpointCompletionRaceTest {
    @Test
    void duplicateObserversCanCompleteTheSameAttemptWithoutDuplicateCleanup() {
        try (Fixture f = new Fixture()) {
            var first = f.watch();
            var second = f.watch();
            f.observation.complete(new K8sManager.JobStatus("succeeded", null));
            assertTrue(first.isDone());
            assertTrue(second.isDone());
            first.join();
            second.join();
            f.k8s.verify(() -> K8sManager.deleteYaml("old-yaml"), times(1));
        }
    }

    @Test
    void staleMissingJobCheckDoesNotOverwritePersistedSuccessOrStartARetry() {
        try (Fixture f = new Fixture()) {
            Checkpoint stale = checkpoint("old-yaml", 1);
            assertTrue(f.db.completeCheckpoint(f.db.getCheckpoint("rank", "v1"), Consts.CHECKPOINT_STATUS_SUCCEEDED));
            f.k8s.when(() -> K8sManager.checkJobsStatusDetailFromYaml("old-yaml"))
                    .thenReturn(new K8sManager.JobStatus("failed", "Job not found"));

            assertTrue(ModelManager.canReuseCreatedCheckpoint(f.db, stale));
            assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, stale.getStatus());
            assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, f.store.getCheckpoint("rank", "v1").getStatus());
        }
    }

    @Test
    void deletionBetweenReadingAndSavingDoesNotResurrectTheCheckpoint() {
        try (Fixture f = new Fixture()) {
            f.onCompletionRead = () -> f.store.deleteCheckpoint("rank", "v1");
            var operation = f.watch();
            f.observation.complete(new K8sManager.JobStatus("succeeded", null));

            assertThrows(CompletionException.class, operation::join);
            assertNull(f.store.getCheckpoint("rank", "v1"));
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void retryBetweenReadingAndSavingIsNotOverwrittenByTheOldCompletion() {
        try (Fixture f = new Fixture()) {
            Checkpoint retry = checkpoint("new-yaml", 2);
            f.onCompletionRead = () -> f.store.upsertCheckpoint(retry);
            var operation = f.watch();
            f.observation.complete(new K8sManager.JobStatus("failed", "old Job failed"));

            assertThrows(CompletionException.class, operation::join);
            assertSame(retry, f.store.getCheckpoint("rank", "v1"));
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, retry.getStatus());
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    private static Checkpoint checkpoint(String yaml, long updatedAt) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setModelName("rank");
        checkpoint.setCheckpointName("v1");
        checkpoint.setYaml(yaml);
        checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
        checkpoint.setUpdatedAt(updatedAt);
        return checkpoint;
    }

    private static final class Fixture implements AutoCloseable {
        final InMemoryStoreAccess store = spy(new InMemoryStoreAccess(List.of(), List.of(), List.of()));
        final MetadataAccess db = new MetadataAccess(null, store, null);
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);
        final CompletableFuture<K8sManager.JobStatus> observation = new CompletableFuture<>();
        Runnable onCompletionRead = () -> {};

        Fixture() {
            store.insertCheckpoint(checkpoint("old-yaml", 1));
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            k8s.when(() -> K8sManager.watchJobsFromYaml("old-yaml")).thenReturn(observation);
            AtomicInteger reads = new AtomicInteger();
            doAnswer(call -> {
                Checkpoint result = (Checkpoint) call.callRealMethod();
                if (reads.incrementAndGet() == 2) onCompletionRead.run();
                return result;
            }).when(store).getCheckpoint("rank", "v1");
        }

        CompletableFuture<Void> watch() {
            return ModelManager.watchCheckpointCompletion(List.of(new CheckpointInfo("rank", "v1")), Runnable::run);
        }

        @Override public void close() {
            k8s.close();
            metadata.close();
        }
    }
}
