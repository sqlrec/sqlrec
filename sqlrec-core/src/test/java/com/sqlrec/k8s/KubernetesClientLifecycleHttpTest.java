package com.sqlrec.k8s;

import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.internal.SerialExecutor;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class KubernetesClientLifecycleHttpTest {
    @Test
    void queuedCallbackHandlesUnderlyingExecutorShutdown() {
        var scheduled = new ArrayDeque<Runnable>();
        var accepting = new AtomicBoolean(true);
        var queuedCallbackRan = new AtomicBoolean();
        SerialExecutor callbacks = new SerialExecutor(task -> {
            if (!accepting.get()) throw new RejectedExecutionException("Client executor has closed");
            scheduled.add(task);
        });
        callbacks.execute(() -> {});
        callbacks.execute(() -> queuedCallbackRan.set(true));

        accepting.set(false);
        // The first callback's finally block tries to submit the second callback.
        assertDoesNotThrow(scheduled.remove()::run);
        assertTrue(scheduled.isEmpty());
        assertFalse(queuedCallbackRan.get());
    }

    @RepeatedTest(value = 30, failureThreshold = 1)
    void deletionWaitFollowedImmediatelyByClientCloseDoesNotDeadlock() throws Exception {
        try (var api = new TestApi(true)) {
            assertNull(api.resource().waitUntilCondition(Objects::isNull, 5, TimeUnit.SECONDS));
            assertTrue(api.watching.await(5, TimeUnit.SECONDS), "Deletion must come from the watch event");
            // Fabric8 #5822: this races the informer's automatic stop on its callback thread.
            // closeClient bounds the close even if a monitor deadlock cannot be interrupted.
        }
    }

    @Test
    void deletionWaitStillHonorsTimeout() throws Exception {
        try (var api = new TestApi(false)) {
            assertThrows(KubernetesClientTimeoutException.class,
                    () -> api.resource().waitUntilCondition(Objects::isNull, 1, TimeUnit.SECONDS));
        }
    }

    @Test
    void deletionWaitPreservesTheInterruptFlag() throws Exception {
        try (var api = new TestApi(false)) {
            var completion = new CompletableFuture<Void>();
            Thread waiter = Thread.startVirtualThread(() -> {
                try {
                    KubernetesClientException error = assertThrows(KubernetesClientException.class,
                            () -> api.resource().waitUntilCondition(Objects::isNull, 30, TimeUnit.SECONDS));
                    assertInstanceOf(InterruptedException.class, error.getCause());
                    assertTrue(Thread.currentThread().isInterrupted());
                    completion.complete(null);
                } catch (Throwable failure) {
                    completion.completeExceptionally(failure);
                }
            });
            try {
                assertTrue(api.watching.await(5, TimeUnit.SECONDS));
                waiter.interrupt();
                completion.get(5, TimeUnit.SECONDS);
            } finally {
                waiter.interrupt();
                // The interrupted SDK wait leaves a watch active. Close its TCP connection before client cleanup.
                api.shutdown.countDown();
                api.server.stop(0);
            }
        }
    }

    private static final class TestApi implements AutoCloseable {
        private static final String CONFIG_MAP = "{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":"
                + "{\"name\":\"cfg\",\"namespace\":\"test\",\"uid\":\"uid\",\"resourceVersion\":\"1\"}}";
        private final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        private final ExecutorService handlers = Executors.newVirtualThreadPerTaskExecutor();
        private final ConcurrentLinkedQueue<Throwable> backgroundFailures = new ConcurrentLinkedQueue<>();
        private final List<Thread> clientThreads = new CopyOnWriteArrayList<>();
        private final ExecutorService clientExecutor = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "kubernetes-lifecycle-test-callback");
            thread.setUncaughtExceptionHandler((ignored, failure) -> backgroundFailures.add(failure));
            clientThreads.add(thread);
            return thread;
        });
        private final CountDownLatch watching = new CountDownLatch(1);
        private final CountDownLatch shutdown = new CountDownLatch(1);
        private final KubernetesClient client;

        TestApi(boolean deleteOnWatch) throws Exception {
            server.setExecutor(handlers);
            server.createContext("/", exchange -> {
                try (exchange) {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    String query = exchange.getRequestURI().getRawQuery();
                    if (query != null && query.contains("watch=true")) {
                        exchange.sendResponseHeaders(200, 0);
                        String event = deleteOnWatch ? "{\"type\":\"DELETED\",\"object\":" + CONFIG_MAP + "}\n"
                                : "{\"type\":\"BOOKMARK\",\"object\":" + CONFIG_MAP + "}\n";
                        exchange.getResponseBody().write(event.getBytes(StandardCharsets.UTF_8));
                        exchange.getResponseBody().flush();
                        watching.countDown();
                        shutdown.await(10, TimeUnit.SECONDS);
                    } else {
                        byte[] list = ("{\"apiVersion\":\"v1\",\"kind\":\"ConfigMapList\","
                                + "\"metadata\":{\"resourceVersion\":\"1\"},\"items\":[" + CONFIG_MAP + "]}")
                                .getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, list.length);
                        exchange.getResponseBody().write(list);
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                }
            });
            server.start();
            client = new KubernetesClientBuilder().withConfig(new ConfigBuilder()
                    .withMasterUrl("http://127.0.0.1:" + server.getAddress().getPort())
                    .withNamespace("test").withOnlyHttpWatches(true).withRequestTimeout(5000).build())
                    .withTaskExecutorSupplier(new KubernetesClientBuilder.ExecutorSupplier() {
                        @Override public Executor get() { return clientExecutor; }
                        // Match Fabric8's default shutdown behavior while capturing uncaught failures.
                        @Override public void onClose(Executor executor) { clientExecutor.shutdownNow(); }
                    }).build();
        }

        Resource<ConfigMap> resource() {
            return client.configMaps().inNamespace("test").withName("cfg");
        }

        @Override
        public void close() throws Exception {
            try {
                KubernetesTestSupport.closeClient(client);
                assertTrue(clientExecutor.awaitTermination(5, TimeUnit.SECONDS),
                        "Client callback executor must terminate");
                // awaitTermination can finish before a worker's uncaught exception handler runs.
                for (Thread thread : clientThreads) {
                    thread.join(1000);
                    assertFalse(thread.isAlive(), "Client callback thread must terminate");
                }
                assertTrue(backgroundFailures.isEmpty(), () -> "Uncaught client callback failures: " + backgroundFailures);
            } finally {
                clientExecutor.shutdownNow();
                shutdown.countDown();
                server.stop(0);
                handlers.close();
            }
        }
    }
}
