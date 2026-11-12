package com.sqlrec.k8s;

import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KubernetesClientLifecycleHttpTest {
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
                // An interrupted SDK wait leaves the watch active. End its HTTP stream before closing the client.
                api.shutdown.countDown();
            }
        }
    }

    private static final class TestApi implements AutoCloseable {
        private static final String CONFIG_MAP = "{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":"
                + "{\"name\":\"cfg\",\"namespace\":\"test\",\"uid\":\"uid\",\"resourceVersion\":\"1\"}}";
        private final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        private final ExecutorService handlers = Executors.newVirtualThreadPerTaskExecutor();
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
                    .withNamespace("test").withOnlyHttpWatches(true).withRequestTimeout(5000).build()).build();
        }

        Resource<ConfigMap> resource() {
            return client.configMaps().inNamespace("test").withName("cfg");
        }

        @Override
        public void close() throws Exception {
            try {
                KubernetesTestSupport.closeClient(client);
            } finally {
                shutdown.countDown();
                server.stop(0);
                handlers.close();
            }
        }
    }
}
