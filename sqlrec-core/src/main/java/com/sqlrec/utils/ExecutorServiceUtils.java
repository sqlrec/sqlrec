package com.sqlrec.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServiceUtils {
    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public static ExecutorService getExecutorService() {
        return executorService;
    }
}
