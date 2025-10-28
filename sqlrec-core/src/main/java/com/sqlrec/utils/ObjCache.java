package com.sqlrec.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;


public class ObjCache<T> {
    private static final Logger log = LoggerFactory.getLogger(ObjCache.class);
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private T obj;
    private long lastUpdateTimeInMillis;
    private long cacheExpireTimeInMillis;
    private boolean asyncUpdate;
    private Supplier<T> updateFunction;

    public ObjCache(long cacheExpireTimeInMillis, boolean asyncUpdate, Supplier<T> updateFunction) {
        this.cacheExpireTimeInMillis = cacheExpireTimeInMillis;
        this.asyncUpdate = asyncUpdate;
        this.updateFunction = updateFunction;
    }

    public T getObj() {
        if (obj == null || System.currentTimeMillis() - lastUpdateTimeInMillis > cacheExpireTimeInMillis) {
            updateObj();
        }
        return obj;
    }

    private synchronized void updateObj() {
        long currentTimeInMillis = System.currentTimeMillis();
        if (currentTimeInMillis - lastUpdateTimeInMillis < cacheExpireTimeInMillis) {
            return;
        }
        lastUpdateTimeInMillis = currentTimeInMillis;

        if (obj == null || !asyncUpdate) {
            obj = updateFunction.get();
        } else {
            executorService.submit(() -> {
                try {
                    obj = updateFunction.get();
                } catch (Exception e) {
                    log.error("Error while updating object in ObjCache", e);
                }
            });
        }
    }
}
