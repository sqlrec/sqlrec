package com.sqlrec.k8s;

import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.http.BasicBuilder;
import io.fabric8.kubernetes.client.http.HttpRequest;
import io.fabric8.kubernetes.client.http.Interceptor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class K8sManagerDryRunRequestTest {
    @Test
    @SilenceLoggers(K8sManager.class)
    void realClientAddsDryRunOnlyToValidationRequests() {
        String yaml = """
                apiVersion: batch/v1
                kind: Job
                metadata: {name: train, namespace: test}
                """;
        for (boolean validate : new boolean[]{true, false}) {
            AtomicReference<HttpRequest> request = new AtomicReference<>();
            var config = new ConfigBuilder().withMasterUrl("http://127.0.0.1:1")
                    .withNamespace("test").build();
            var client = new KubernetesClientBuilder().withConfig(config)
                    .withHttpClientBuilderConsumer(builder -> builder.addOrReplaceInterceptor("capture", new Interceptor() {
                        @Override
                        public void before(BasicBuilder builder, HttpRequest value, RequestTags tags) {
                            request.set(value);
                            // Exercise the real SDK's request generation without opening a socket.
                            throw new IllegalStateException("intercepted before network");
                        }
                    })).build();
            K8sManager.setKubernetesClientForTest(client);
            try {
                RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                    if (validate) K8sManager.validateYaml(yaml);
                    else K8sManager.applyYaml(yaml);
                });
                assertTrue(failure.getMessage().contains("intercepted before network"));
                assertNotNull(request.get());
                assertEquals("PATCH", request.get().method());
                assertEquals("/apis/batch/v1/namespaces/test/jobs/train", request.get().uri().getPath());
                String query = request.get().uri().getQuery();
                assertTrue(query.contains("fieldManager=fabric8"));
                assertEquals(validate, query.contains("dryRun=All"));
            } finally {
                K8sManager.resetClient();
            }
        }
    }
}
