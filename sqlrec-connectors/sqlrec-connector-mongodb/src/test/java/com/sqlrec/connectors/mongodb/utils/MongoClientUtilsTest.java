package com.sqlrec.connectors.mongodb.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.handler.MongoHandler;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongoClientUtilsTest {
    @AfterEach
    void closeClients() {
        MongoClientUtils.closeAll();
    }

    @Test
    void clientCreatedDuringCloseSurvivesThatClose() throws Exception {
        String uri = "mongodb://localhost:27017";
        MongoClient old = mock(MongoClient.class);
        MongoClient replacement = mock(MongoClient.class);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch finishClose = new CountDownLatch(1);
        doAnswer(call -> {
            closeStarted.countDown();
            assertTrue(finishClose.await(5, TimeUnit.SECONDS));
            return null;
        }).when(old).close();
        try (MockedStatic<MongoClients> factory = mockStatic(MongoClients.class)) {
            factory.when(() -> MongoClients.create(uri)).thenReturn(old, replacement);
            assertSame(old, MongoClientUtils.getOrCreate(uri));

            CompletableFuture<Void> closing = CompletableFuture.runAsync(MongoClientUtils::closeAll);
            try {
                assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
                assertSame(replacement, MongoClientUtils.getOrCreate(uri));
            } finally {
                finishClose.countDown();
                closing.get(5, TimeUnit.SECONDS);
            }
            assertSame(replacement, MongoClientUtils.getOrCreate(uri));
            verify(replacement, never()).close();
        }
    }

    @Test
    void handlersWithTheSameUriShareAClientAcrossDatabasesAndCollections() {
        String uri = "mongodb://localhost:27017";
        MongoClient client = mock(MongoClient.class);
        MongoDatabase firstDatabase = mock(MongoDatabase.class);
        MongoDatabase secondDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> firstCollection = mock(MongoCollection.class);
        MongoCollection<Document> secondCollection = mock(MongoCollection.class);
        when(client.getDatabase("first")).thenReturn(firstDatabase);
        when(client.getDatabase("second")).thenReturn(secondDatabase);
        when(firstDatabase.getCollection("users")).thenReturn(firstCollection);
        when(secondDatabase.getCollection("items")).thenReturn(secondCollection);

        try (MockedStatic<MongoClients> factory = mockStatic(MongoClients.class)) {
            factory.when(() -> MongoClients.create(uri)).thenReturn(client);
            assertTrue(new MongoHandler(config(uri, "first", "users")).delete(new Object[]{1}));
            assertTrue(new MongoHandler(config(uri, "second", "items")).delete(new Object[]{2}));
            assertSame(client, MongoClientUtils.getOrCreate(uri));
            factory.verify(() -> MongoClients.create(uri), times(1));
            verify(firstCollection).deleteOne(any(Bson.class));
            verify(secondCollection).deleteOne(any(Bson.class));
        }
    }

    @Test
    @SilenceLoggers(MongoClientUtils.class)
    void closeAllClearsTheCacheEvenIfOneClientFailsToClose() {
        String uri = "mongodb://localhost:27017";
        String otherUri = "mongodb://localhost:27018";
        MongoClient first = mock(MongoClient.class);
        MongoClient second = mock(MongoClient.class);
        MongoClient replacement = mock(MongoClient.class);
        try (MockedStatic<MongoClients> factory = mockStatic(MongoClients.class)) {
            factory.when(() -> MongoClients.create(uri)).thenReturn(first, replacement);
            factory.when(() -> MongoClients.create(otherUri)).thenReturn(second);
            MongoClientUtils.getOrCreate(uri);
            MongoClientUtils.getOrCreate(otherUri);
            doThrow(new RuntimeException("close failed")).when(first).close();
            assertDoesNotThrow(MongoClientUtils::closeAll);
            verify(first).close();
            verify(second).close();
            MongoClientUtils.closeAll();
            verify(first, times(1)).close();
            assertSame(replacement, MongoClientUtils.getOrCreate(uri));
        }
    }

    private static MongoConfig config(String uri, String database, String collection) {
        MongoConfig config = new MongoConfig();
        config.uri = uri;
        config.database = database;
        config.collection = collection;
        config.primaryKey = "id";
        config.primaryKeyIndex = 0;
        return config;
    }
}
