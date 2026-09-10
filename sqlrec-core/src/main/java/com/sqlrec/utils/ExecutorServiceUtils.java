package com.sqlrec.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServiceUtils {
    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private static final ExecutorService cacheRefreshExecutorService =
            Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cache-refresh");
                thread.setDaemon(true);
                return thread;
            });

    public static ExecutorService getExecutorService() {
        return executorService;
    }

    public static ExecutorService getCacheRefreshExecutorService() {
        return cacheRefreshExecutorService;
    }
}
