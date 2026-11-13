package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.utils.ExecutorServiceUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/** Observes checkpoint attempts and reconciles their completion before consumption or retry. */
final class CheckpointCompletion {
    private CheckpointCompletion() {
    }

    static CompletableFuture<Void> watch(List<CheckpointInfo> checkpointInfos, Executor executor) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            if (checkpointInfos == null || checkpointInfos.isEmpty()) return CompletableFuture.completedFuture(null);
            MetadataAccess db = MetadataAccessFactory.getInstance();
            Map<String, List<CheckpointAttempt>> groups = new HashMap<>();
            for (CheckpointInfo info : checkpointInfos) {
                Checkpoint checkpoint = db.getCheckpoint(info.getModelName(), info.getCheckpointName());
                if (checkpoint == null) throw new IllegalArgumentException("Checkpoint not found: " + info.getCheckpointName());
                if (Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(checkpoint.getStatus())) continue;
                if (Consts.CHECKPOINT_STATUS_FAILED.equals(checkpoint.getStatus())) {
                    throw new IllegalStateException("Checkpoint failed: " + info.getCheckpointName());
                }
                if (!Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())) {
                    throw new IllegalStateException("Unknown checkpoint status: " + checkpoint.getStatus());
                }
                groups.computeIfAbsent(Objects.requireNonNullElse(checkpoint.getYaml(), ""), ignored -> new ArrayList<>())
                        .add(new CheckpointAttempt(info.getModelName(), info.getCheckpointName(), checkpoint.getUpdatedAt()));
            }
            if (groups.isEmpty()) return CompletableFuture.completedFuture(null);
            AtomicInteger remaining = new AtomicInteger(groups.size());
            for (var group : groups.entrySet()) {
                if (completion.isDone()) break;
                String yaml = group.getKey();
                CompletableFuture<K8sManager.JobStatus> observation = yaml.isEmpty()
                        ? CompletableFuture.completedFuture(new K8sManager.JobStatus("failed", "k8sYaml is empty"))
                        : K8sManager.watchJobsFromYaml(yaml);
                completion.whenComplete((result, failure) -> observation.cancel(false));
                observation.thenAcceptAsync(status -> {
                    if (completion.isDone()) return;
                    finishCheckpointGroup(db, yaml, group.getValue(), status);
                    if (remaining.decrementAndGet() == 0) completion.complete(null);
                }, executor).whenComplete((ignored, failure) -> {
                    // Includes failure to submit the callback, not just failures inside it.
                    if (failure != null) completion.completeExceptionally(ExecutorServiceUtils.unwrap(failure));
                });
            }
        } catch (Exception failure) {
            completion.completeExceptionally(failure);
        }
        return completion;
    }

    private record CheckpointAttempt(String modelName, String checkpointName, long updatedAt) {}

    private static void finishCheckpointGroup(
            MetadataAccess db, String yaml, List<CheckpointAttempt> attempts, K8sManager.JobStatus status) {
        List<Checkpoint> checkpoints = new ArrayList<>();
        for (CheckpointAttempt attempt : attempts) {
            Checkpoint checkpoint = db.getCheckpoint(attempt.modelName(), attempt.checkpointName());
            if (checkpoint == null) throw new IllegalStateException("Checkpoint was deleted: " + attempt.checkpointName());
            if (!yaml.equals(Objects.requireNonNullElse(checkpoint.getYaml(), ""))
                    || (Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())
                    && checkpoint.getUpdatedAt() != attempt.updatedAt())) {
                throw new IllegalStateException("Checkpoint was replaced: " + attempt.checkpointName());
            }
            checkpoints.add(checkpoint);
        }
        List<String> failures = new ArrayList<>();
        boolean updated = false;
        for (Checkpoint checkpoint : checkpoints) {
            if (Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())) {
                updated |= updateCheckpointStatus(db, checkpoint, "succeeded".equals(status.state())
                        ? Consts.CHECKPOINT_STATUS_SUCCEEDED : Consts.CHECKPOINT_STATUS_FAILED);
            }
            if (Consts.CHECKPOINT_STATUS_FAILED.equals(checkpoint.getStatus())) failures.add(checkpoint.getCheckpointName());
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("Checkpoints failed: " + String.join(", ", failures)
                    + (status.detail() == null ? "" : " (" + status.detail() + ")"));
        }
        // One shared Job may produce multiple checkpoints. Persist every status before cleanup.
        if (updated) K8sManager.deleteYaml(yaml);
    }

    /** Reconcile an unfinished checkpoint before consuming it, without deleting shared Jobs. */
    static void refresh(MetadataAccess db, Checkpoint checkpoint) {
        if (!Consts.CHECKPOINT_STATUS_CREATED.equals(checkpoint.getStatus())) {
            return;
        }
        String yaml = checkpoint.getYaml();
        K8sManager.JobStatus status = StringUtils.isEmpty(yaml)
                ? new K8sManager.JobStatus("failed", "k8sYaml is empty")
                : K8sManager.checkJobsStatusDetailFromYaml(yaml);
        if ("succeeded".equals(status.state())) {
            updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_SUCCEEDED);
        } else if ("failed".equals(status.state())) {
            updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_FAILED);
        }
    }

    static boolean canReuseCreated(MetadataAccess db, Checkpoint checkpoint) {
        String yaml = checkpoint.getYaml();
        if (!StringUtils.isEmpty(yaml)
                && !"failed".equals(K8sManager.checkJobsStatusDetailFromYaml(yaml).state())) {
            return true;
        }
        updateCheckpointStatus(db, checkpoint, Consts.CHECKPOINT_STATUS_FAILED);
        return Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(checkpoint.getStatus());
    }

    private static boolean updateCheckpointStatus(
            MetadataAccess db,
            Checkpoint checkpoint,
            String status
    ) {
        if (db.completeCheckpoint(checkpoint, status)) return true;
        Checkpoint current = db.getCheckpoint(checkpoint.getModelName(), checkpoint.getCheckpointName());
        if (current == null) throw new IllegalStateException("Checkpoint was deleted: " + checkpoint.getCheckpointName());
        if (!Objects.equals(current.getYaml(), checkpoint.getYaml())
                || (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(current.getStatus())
                && !Consts.CHECKPOINT_STATUS_FAILED.equals(current.getStatus()))) {
            throw new IllegalStateException("Checkpoint was replaced: " + checkpoint.getCheckpointName());
        }
        // Another observer may already have persisted this attempt's terminal state.
        checkpoint.setStatus(current.getStatus());
        checkpoint.setUpdatedAt(current.getUpdatedAt());
        return false;
    }
}
