package com.sqlrec.k8s;

import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.RepeatedTest;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JobCompletionWatcherHttpTest {
    @RepeatedTest(value = 30, failureThreshold = 1)
    void realInformerListsOnceAndCompletesFromAWatchEvent() throws Exception {
        Job running = new JobBuilder().withNewMetadata().withName("train").withNamespace("test")
                .withUid("uid").withResourceVersion("1").endMetadata()
                .withNewSpec().withCompletions(1).endSpec().withNewStatus().withActive(1).endStatus().build();
        Job succeeded = new JobBuilder(running).editMetadata().withResourceVersion("2").endMetadata()
                .editStatus().withSucceeded(1).endStatus().build();
        AtomicInteger jobLists = new AtomicInteger();
        CountDownLatch watching = new CountDownLatch(1);
        CountDownLatch podsWatching = new CountDownLatch(1);
        CountDownLatch finishJob = new CountDownLatch(1);
        CountDownLatch shutdown = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try (var handlers = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setExecutor(handlers);
            server.createContext("/", exchange -> {
                try (exchange) {
                    boolean jobs = exchange.getRequestURI().getPath().endsWith("/jobs");
                    String query = exchange.getRequestURI().getRawQuery();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    if (query != null && query.contains("watch=true")) {
                        exchange.sendResponseHeaders(200, 0);
                        String bookmark = "{\"type\":\"BOOKMARK\",\"object\":{\"apiVersion\":\""
                                + (jobs ? "batch/v1" : "v1") + "\",\"kind\":\"" + (jobs ? "Job" : "Pod")
                                + "\",\"metadata\":{\"resourceVersion\":\"1\"}}}\n";
                        exchange.getResponseBody().write(bookmark.getBytes(StandardCharsets.UTF_8));
                        exchange.getResponseBody().flush();
                        if (jobs) {
                            watching.countDown();
                            if (finishJob.await(5, TimeUnit.SECONDS)) {
                                String event = "{\"type\":\"MODIFIED\",\"object\":" + Serialization.asJson(succeeded) + "}\n";
                                exchange.getResponseBody().write(event.getBytes(StandardCharsets.UTF_8));
                                exchange.getResponseBody().flush();
                            }
                        } else {
                            podsWatching.countDown();
                        }
                        shutdown.await(10, TimeUnit.SECONDS);
                    } else {
                        if (jobs) jobLists.incrementAndGet();
                        String body = jobs
                                ? "{\"kind\":\"JobList\",\"apiVersion\":\"batch/v1\",\"metadata\":{\"resourceVersion\":\"1\"},\"items\":["
                                + Serialization.asJson(running) + "]}"
                                : "{\"kind\":\"PodList\",\"apiVersion\":\"v1\",\"metadata\":{\"resourceVersion\":\"1\"},\"items\":[]}";
                        byte[] data = body.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, data.length);
                        exchange.getResponseBody().write(data);
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                }
            });
            server.start();
            var config = new ConfigBuilder().withMasterUrl("http://127.0.0.1:" + server.getAddress().getPort())
                    // This server implements streaming HTTP, not the WebSocket upgrade protocol.
                    .withOnlyHttpWatches(true).withNamespace("test").withRequestTimeout(5000).build();
            var client = new KubernetesClientBuilder().withConfig(config).build();
            try {
                var target = new JobBuilder().withNewMetadata().withName("train").withNamespace("test").endMetadata().build();
                var completion = JobCompletionWatcher.watch(client, target);
                try {
                    assertTrue(watching.await(5, TimeUnit.SECONDS));
                    assertTrue(podsWatching.await(5, TimeUnit.SECONDS));
                    assertFalse(completion.isDone());
                    finishJob.countDown();
                    assertEquals("succeeded", completion.get(5, TimeUnit.SECONDS).state());
                    assertEquals(1, jobLists.get());
                } finally {
                    completion.cancel(false);
                    shutdown.countDown();
                }
            } finally {
                shutdown.countDown();
                try {
                    KubernetesTestSupport.closeClient(client);
                } finally {
                    server.stop(0);
                }
            }
        }
    }
}
