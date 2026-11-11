package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.model.ModelManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ModelCompletionFutureTest {
    @Test
    void synchronousTimeoutDoesNotFailOrCancelTheOperation() throws Exception {
        long previousTimeout = SqlRecConfigs.SQL_SYNC_EXECUTE_TIMEOUT.getDefaultValue();
        try (var models = mockStatic(ModelManager.class); var schemas = mockStatic(CalciteSchemaFactory.class)) {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            List<CheckpointInfo> checkpoints = List.of(new CheckpointInfo("rank", "v1"));
            models.when(() -> ModelManager.watchCheckpointCompletion(checkpoints)).thenReturn(completion);
            schemas.when(CalciteSchemaFactory::createCalciteSchema).thenReturn(CalciteSchema.createRootSchema(false));
            ModelSqlProcessResult result = ModelSqlProcessResult.msg("submitted", "msg", checkpoints);
            SqlExecutor executor = new SqlExecutor() {
                @Override public SqlProcessResult executeSqlAsync(String sql) { return result; }
            };
            SqlRecConfigs.SQL_SYNC_EXECUTE_TIMEOUT.setDefaultValue(20L);
            RuntimeException failure = assertThrows(RuntimeException.class, () -> executor.executeSql("TRAIN"));
            assertTrue(failure.getMessage().contains("20ms"));
            assertFalse(completion.isDone());
            assertFalse(result.isCompleted());
            assertFalse(result.isCompleted());
            completion.complete(null);
            assertTrue(result.isCompleted());
            assertNotNull(executor.executeSql("TRAIN"));
            models.verify(() -> ModelManager.watchCheckpointCompletion(checkpoints), times(1));
            models.verifyNoMoreInteractions();
        } finally {
            SqlRecConfigs.SQL_SYNC_EXECUTE_TIMEOUT.setDefaultValue(previousTimeout);
        }
    }

    @Test
    void failureIsReportedAndClosingTheResultCancelsOnlyItsObservation() {
        try (var models = mockStatic(ModelManager.class)) {
            List<CheckpointInfo> checkpoints = List.of(new CheckpointInfo("rank", "v1"));
            CompletableFuture<Void> completion = new CompletableFuture<>();
            models.when(() -> ModelManager.watchCheckpointCompletion(checkpoints)).thenReturn(completion);
            var result = ModelSqlProcessResult.msg("submitted", "msg", checkpoints);
            RuntimeException failure = new RuntimeException("trainer failed");
            completion.completeExceptionally(failure);
            assertSame(failure, assertThrows(CompletionException.class, result::isCompleted).getCause());

            CompletableFuture<Void> next = new CompletableFuture<>();
            models.when(() -> ModelManager.watchCheckpointCompletion(checkpoints)).thenReturn(next);
            result.setCheckpointInfos(checkpoints);
            result.close();
            assertTrue(next.isCancelled());
        }
    }

    @Test
    void interruptedWaitRetainsTheOperationAndRestoresTheInterruptFlag() throws Exception {
        try (var models = mockStatic(ModelManager.class); var schemas = mockStatic(CalciteSchemaFactory.class)) {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            List<CheckpointInfo> checkpoints = List.of(new CheckpointInfo("rank", "v1"));
            models.when(() -> ModelManager.watchCheckpointCompletion(checkpoints)).thenReturn(completion);
            schemas.when(CalciteSchemaFactory::createCalciteSchema).thenReturn(CalciteSchema.createRootSchema(false));
            CountDownLatch waiting = new CountDownLatch(1);
            ModelSqlProcessResult result = new ModelSqlProcessResult(null, null, checkpoints) {
                @Override
                public void awaitCompletion(long timeout) throws InterruptedException,
                        java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
                    waiting.countDown();
                    super.awaitCompletion(timeout);
                }
            };
            SqlExecutor executor = new SqlExecutor() {
                @Override public SqlProcessResult executeSqlAsync(String sql) { return result; }
            };
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread caller = Thread.ofVirtual().start(() -> {
                try { executor.executeSql("TRAIN"); }
                catch (Throwable error) { failure.set(error); interrupted.set(Thread.currentThread().isInterrupted()); }
            });
            assertTrue(waiting.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(5000);
            assertFalse(caller.isAlive());
            assertInstanceOf(InterruptedException.class, failure.get());
            assertTrue(interrupted.get());
            assertFalse(completion.isDone());
            result.close();
        }
    }
}
