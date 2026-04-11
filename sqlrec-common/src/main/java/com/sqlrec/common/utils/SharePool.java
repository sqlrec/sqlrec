package com.sqlrec.common.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class SharePool<T> {
    private final Object[] objects;
    private final Supplier<T> supplier;
    private final AtomicLong counter;
    private final int size;

    public SharePool(int size, Supplier<T> supplier) {
        if (size <= 0) {
            throw new IllegalArgumentException("Pool size must be positive");
        }
        if (supplier == null) {
            throw new IllegalArgumentException("Supplier cannot be null");
        }
        this.size = size;
        this.objects = new Object[size];
        this.supplier = supplier;
        this.counter = new AtomicLong(0);
    }

    @SuppressWarnings("unchecked")
    public T getObject() {
        long index = counter.getAndIncrement();
        int arrayIndex = (int) (index % size);
        
        if (objects[arrayIndex] == null) {
            synchronized (this) {
                if (objects[arrayIndex] == null) {
                    objects[arrayIndex] = supplier.get();
                }
            }
        }
        
        return (T) objects[arrayIndex];
    }

    public int getSize() {
        return size;
    }

    public int getCreatedCount() {
        int count = 0;
        for (Object obj : objects) {
            if (obj != null) {
                count++;
            }
        }
        return count;
    }
}
