package com.sqlrec.k8s;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class K8sManagerApplyTest {
    @Test
    void validatedYamlKeepsItsNamespaceAfterTheClientChanges() throws Exception {
        String yaml = "apiVersion: batch/v1\nkind: Job\nmetadata: {name: train}\n";
        List<String> requests = new ArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                String method = exchange.getRequestMethod();
                requests.add(method + " " + exchange.getRequestURI());
                exchange.getRequestBody().readAllBytes();
                String response = method.equals("DELETE")
                        ? """
                          {"apiVersion":"v1","kind":"Status","status":"Success"}
                          """
                        : """
                          {"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"train","namespace":"original"},
                           "status":{"succeeded":1}}
                          """;
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        try {
            var config = new ConfigBuilder()
                    .withMasterUrl("http://127.0.0.1:" + server.getAddress().getPort())
                    .withRequestTimeout(2000).withConnectionTimeout(2000);
            K8sManager.setKubernetesClientForTest(
                    new KubernetesClientBuilder().withConfig(config.withNamespace("original").build()).build());
            String validated = K8sManager.validateYaml(yaml);
            assertEquals("original", K8sYamlUtils.parseK8sYamlAndGetJobs(validated).getFirst().getMetadata().getNamespace());

            K8sManager.setKubernetesClientForTest(
                    new KubernetesClientBuilder().withConfig(config.withNamespace("other").build()).build());
            K8sManager.applyYaml(validated);
            assertEquals(K8sManager.JobState.SUCCEEDED, K8sManager.checkJobsStatusDetailFromYaml(validated).state());
            K8sManager.deleteYaml(validated);

            String path = "/apis/batch/v1/namespaces/original/jobs/train";
            assertEquals(List.of("PATCH", "PATCH", "GET", "DELETE"), requests.stream()
                    .map(request -> request.substring(0, request.indexOf(' '))).toList());
            assertTrue(requests.stream().allMatch(request -> request.contains(path)), requests.toString());
            assertTrue(requests.getFirst().contains("dryRun=All"));
            assertFalse(requests.get(1).contains("dryRun=All"));
        } finally {
            K8sManager.resetClient();
            server.stop(0);
        }
    }

    @Test
    void customResourceInListIsDeletedInItsOriginalNamespace() throws Exception {
        String yaml = """
                apiVersion: v1
                kind: List
                items:
                - apiVersion: review.example/v1
                  kind: WorkItem
                  metadata: {name: train}
                """;
        List<String> writes = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();
                exchange.getRequestBody().readAllBytes();
                String response;
                if (path.equals("/apis/review.example/v1")) {
                    response = """
                            {"apiVersion":"v1","kind":"APIResourceList","groupVersion":"review.example/v1",
                             "resources":[{"name":"workitems","kind":"WorkItem","namespaced":true,
                                           "verbs":["get","list","watch","patch","delete"]}]}
                            """;
                } else {
                    if (method.equals("PATCH") || method.equals("DELETE")) {
                        writes.add(method + " " + path);
                    }
                    response = switch (method) {
                        case "PATCH" -> """
                                {"apiVersion":"review.example/v1","kind":"WorkItem",
                                 "metadata":{"name":"train","namespace":"original"}}
                                """;
                        case "DELETE" -> """
                                {"apiVersion":"v1","kind":"Status","status":"Success"}
                                """;
                        default -> """
                                {"apiVersion":"review.example/v1","kind":"WorkItemList",
                                 "metadata":{"resourceVersion":"1"},"items":[]}
                                """;
                    };
                }
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        try {
            var config = new ConfigBuilder()
                    .withMasterUrl("http://127.0.0.1:" + server.getAddress().getPort())
                    .withRequestTimeout(2000).withConnectionTimeout(2000);
            K8sManager.setKubernetesClientForTest(
                    new KubernetesClientBuilder().withConfig(config.withNamespace("original").build()).build());
            String validated = K8sManager.validateYaml(yaml);
            assertEquals("original", K8sYamlUtils.parseK8sYaml(validated).getFirst().getMetadata().getNamespace());

            K8sManager.setKubernetesClientForTest(
                    new KubernetesClientBuilder().withConfig(config.withNamespace("other").build()).build());
            K8sManager.applyYaml(validated);
            K8sManager.deleteYamlAndWait(validated);

            String path = "/apis/review.example/v1/namespaces/original/workitems/train";
            assertEquals(List.of("PATCH " + path, "PATCH " + path, "DELETE " + path), writes);
        } finally {
            K8sManager.resetClient();
            server.stop(0);
        }
    }

    @Test
    @SilenceLoggers(K8sManager.class)
    void creationPartialFailureRetryAndUpdateAllUseServerSideApply() throws Exception {
        String yaml = """
                apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: serving-config
                  namespace: test
                data:
                  version: first
                ---
                apiVersion: v1
                kind: Service
                metadata:
                  name: serving
                  namespace: test
                spec:
                  selector:
                    app: serving
                  ports:
                  - port: 80
                """;
        Map<String, HasMetadata> resources = new ConcurrentHashMap<>();
        List<String> requests = new ArrayList<>();
        AtomicBoolean failService = new AtomicBoolean(true);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int status = 200;
            String response;
            if (path.endsWith("/services/serving") && failService.getAndSet(false)) {
                status = 422;
                response = "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\","
                        + "\"reason\":\"Invalid\",\"message\":\"simulated apply failure\",\"code\":422}";
            } else {
                HasMetadata resource = Serialization.unmarshal(body);
                resources.put(path, resource);
                response = Serialization.asJson(resource);
            }
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            var config = new ConfigBuilder()
                    .withMasterUrl("http://127.0.0.1:" + server.getAddress().getPort())
                    .withNamespace("test").withRequestTimeout(2000).withConnectionTimeout(2000).build();
            K8sManager.setKubernetesClientForTest(new KubernetesClientBuilder().withConfig(config).build());

            assertThrows(RuntimeException.class, () -> K8sManager.applyYaml(yaml));
            assertEquals(1, resources.size());
            K8sManager.applyYaml(yaml);
            assertEquals(2, resources.size());
            K8sManager.applyYaml(yaml.replace("version: first", "version: second"));
            assertTrue(Serialization.asJson(resources.get("/api/v1/namespaces/test/configmaps/serving-config"))
                    .contains("second"));
            assertEquals(6, requests.size());
            assertTrue(requests.stream().allMatch(request -> request.startsWith("PATCH ")
                    && request.contains("fieldManager=fabric8")), requests.toString());
        } finally {
            K8sManager.resetClient();
            server.stop(0);
        }
    }
}
