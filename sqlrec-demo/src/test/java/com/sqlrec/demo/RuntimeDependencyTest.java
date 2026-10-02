package com.sqlrec.demo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RuntimeDependencyTest {
    @Test
    void initializesNettyAllocatorOnDemoRuntimeClasspath() {
        // Lettuce initializes this allocator when opening a Redis connection.
        // Incompatible netty-buffer/netty-common versions fail before any I/O.
        assertDoesNotThrow(() -> {
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES);
            try {
                buffer.writeLong(42L);
                assertEquals(42L, buffer.readLong());
            } finally {
                buffer.release();
            }
        });
    }
}
