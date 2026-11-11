package com.sqlrec.k8s;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

final class KubernetesTestSupport {
    private KubernetesTestSupport() {}

    static void closeClient(KubernetesClient client) throws Exception {
        // A monitor deadlock cannot be interrupted by JUnit's ordinary timeout.
        try {
            CompletableFuture.runAsync(client::close, task -> {
                Thread closer = new Thread(task, "job-watcher-test-client-close");
                closer.setDaemon(true);
                closer.start();
            }).get(5, TimeUnit.SECONDS);
        } catch (TimeoutException failure) {
            String threads = Arrays.stream(ManagementFactory.getThreadMXBean().dumpAllThreads(true, true))
                    .map(info -> info + Arrays.stream(info.getStackTrace())
                            .map(frame -> "\tat " + frame).collect(Collectors.joining("\n")))
                    .collect(Collectors.joining("\n\n"));
            Path dump = Path.of("target/job-watcher-close-threads.txt");
            Files.writeString(dump, threads);
            throw new AssertionError("Kubernetes client close did not finish; see " + dump, failure);
        }
    }
}
