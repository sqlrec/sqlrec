package com.sqlrec.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Const {
    public static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    public static String DEFAULT_SCHEMA_NAME = "default";
}
