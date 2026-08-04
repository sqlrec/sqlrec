package com.sqlrec.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;

public class ExecutorServiceUtilsTest {

    @Test
    public void testGetExecutorServiceNotNull() {
        ExecutorService executor = ExecutorServiceUtils.getExecutorService();

        assertNotNull(executor);
    }

    @Test
    public void testGetExecutorServiceSingleton() {
        ExecutorService e1 = ExecutorServiceUtils.getExecutorService();
        ExecutorService e2 = ExecutorServiceUtils.getExecutorService();

        assertSame(e1, e2);
    }

    @Test
    public void testExecutorServiceCanExecute() throws Exception {
        ExecutorService executor = ExecutorServiceUtils.getExecutorService();
        int[] counter = {0};

        executor.submit(() -> counter[0]++).get();

        assertEquals(1, counter[0]);
    }
}
