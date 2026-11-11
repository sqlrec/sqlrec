package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.k8s.K8sManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CheckpointCompletionWatchTest {
    @Test
    void rejectedCompletionCallbackFailsTheOperationAndReleasesOtherGroups() throws Exception {
        try (Fixture f = new Fixture()) {
            Checkpoint user = f.checkpoint("user", "shared");
            Checkpoint item = f.checkpoint("item", "other");
            var other = new CompletableFuture<K8sManager.JobStatus>();
            f.k8s.when(() -> K8sManager.watchJobsFromYaml("other")).thenReturn(other);
            var rejected = new RejectedExecutionException("executor stopped");
            var operation = ModelManager.watchCheckpointCompletion(
                    List.of(new CheckpointInfo("rank", "user"), new CheckpointInfo("rank", "item")),
                    task -> { throw rejected; });

            f.observation.complete(new K8sManager.JobStatus("succeeded", null));

            var error = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> operation.get(2, TimeUnit.SECONDS));
            assertSame(rejected, error.getCause());
            assertTrue(other.isCancelled());
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, user.getStatus());
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, item.getStatus());
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void sharedJobUpdatesAllCheckpointsBeforeOneCleanup() {
        try (Fixture f = new Fixture()) {
            Checkpoint user = f.checkpoint("user", "shared");
            Checkpoint item = f.checkpoint("item", "shared");
            f.k8s.when(() -> K8sManager.deleteYaml("shared")).thenAnswer(call -> {
                assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, user.getStatus());
                assertEquals(Consts.CHECKPOINT_STATUS_SUCCEEDED, item.getStatus());
                verify(f.db).completeCheckpoint(user, Consts.CHECKPOINT_STATUS_SUCCEEDED);
                verify(f.db).completeCheckpoint(item, Consts.CHECKPOINT_STATUS_SUCCEEDED);
                return null;
            });
            var operation = f.watch("user", "item");
            assertFalse(operation.isDone());
            f.observation.complete(new K8sManager.JobStatus("succeeded", null));
            operation.join();
            f.k8s.verify(() -> K8sManager.watchJobsFromYaml("shared"), times(1));
            f.k8s.verify(() -> K8sManager.deleteYaml("shared"), times(1));
        }
    }

    @Test
    void jobFailureIsPersistedForAllOutputsAndReportedWithoutDeletingEvidence() {
        try (Fixture f = new Fixture()) {
            Checkpoint user = f.checkpoint("user", "shared");
            Checkpoint item = f.checkpoint("item", "shared");
            var operation = f.watch("user", "item");
            f.observation.complete(new K8sManager.JobStatus("failed", "ImagePullBackOff"));
            var failure = assertThrows(CompletionException.class, operation::join);
            assertTrue(failure.getCause().getMessage().contains("ImagePullBackOff"));
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, user.getStatus());
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, item.getStatus());
            verify(f.db).completeCheckpoint(user, Consts.CHECKPOINT_STATUS_FAILED);
            verify(f.db).completeCheckpoint(item, Consts.CHECKPOINT_STATUS_FAILED);
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void watchFailureDoesNotMarkTheJobFailedAndCancellationOnlyStopsObservation() {
        try (Fixture f = new Fixture()) {
            Checkpoint checkpoint = f.checkpoint("user", "shared");
            var operation = f.watch("user");
            RuntimeException failure = new RuntimeException("watch disconnected permanently");
            f.observation.completeExceptionally(failure);
            assertSame(failure, assertThrows(CompletionException.class, operation::join).getCause());
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, checkpoint.getStatus());
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
        try (Fixture f = new Fixture()) {
            f.checkpoint("user", "shared");
            f.watch("user").cancel(false);
            assertTrue(f.observation.isCancelled());
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void lateEventsCannotOverwriteARetriedCheckpoint() {
        try (Fixture f = new Fixture()) {
            Checkpoint checkpoint = f.checkpoint("user", "shared");
            var operation = f.watch("user");
            checkpoint.setUpdatedAt(2);
            f.observation.complete(new K8sManager.JobStatus("succeeded", null));
            assertTrue(assertThrows(CompletionException.class, operation::join).getCause().getMessage().contains("replaced"));
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, checkpoint.getStatus());
            verify(f.db, never()).upsertCheckpoint(any());
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void persistedCompletionAvoidsAWatchAndWriteFailurePreventsCleanup() {
        try (Fixture f = new Fixture()) {
            f.checkpoint("user", "shared").setStatus(Consts.CHECKPOINT_STATUS_SUCCEEDED);
            f.watch("user").join();
            f.k8s.verifyNoInteractions();
        }
        try (Fixture f = new Fixture()) {
            f.checkpoint("user", "shared");
            doThrow(new IllegalStateException("database unavailable")).when(f.db).completeCheckpoint(any(), anyString());
            var operation = f.watch("user");
            f.observation.complete(new K8sManager.JobStatus("succeeded", null));
            assertThrows(CompletionException.class, operation::join);
            f.k8s.verify(() -> K8sManager.deleteYaml(anyString()), never());
        }
    }

    @Test
    void oneFailedGroupReleasesOtherGroupsAndEmptyYamlFailsWithoutAWatch() {
        try (Fixture f = new Fixture()) {
            f.checkpoint("user", "shared");
            f.checkpoint("item", "other");
            CompletableFuture<K8sManager.JobStatus> other = new CompletableFuture<>();
            f.k8s.when(() -> K8sManager.watchJobsFromYaml("other")).thenReturn(other);
            var operation = f.watch("user", "item");
            f.observation.complete(new K8sManager.JobStatus("failed", "trainer failed"));
            assertThrows(CompletionException.class, operation::join);
            assertTrue(other.isCancelled());
        }
        try (Fixture f = new Fixture()) {
            Checkpoint checkpoint = f.checkpoint("user", null);
            assertThrows(CompletionException.class, () -> f.watch("user").join());
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, checkpoint.getStatus());
            f.k8s.verifyNoInteractions();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MetadataAccess db = mock(MetadataAccess.class);
        final CompletableFuture<K8sManager.JobStatus> observation = new CompletableFuture<>();
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);

        Fixture() {
            CheckpointTestSupport.stubCompletion(db);
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            k8s.when(() -> K8sManager.watchJobsFromYaml("shared")).thenReturn(observation);
        }

        Checkpoint checkpoint(String name, String yaml) {
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setModelName("rank");
            checkpoint.setCheckpointName(name);
            checkpoint.setYaml(yaml);
            checkpoint.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
            checkpoint.setUpdatedAt(1);
            when(db.getCheckpoint("rank", name)).thenReturn(checkpoint);
            return checkpoint;
        }

        CompletableFuture<Void> watch(String... names) {
            return ModelManager.watchCheckpointCompletion(
                    java.util.Arrays.stream(names).map(name -> new CheckpointInfo("rank", name)).toList(), Runnable::run);
        }

        @Override
        public void close() {
            k8s.close();
            metadata.close();
        }
    }
}
