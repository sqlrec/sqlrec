package com.sqlrec.common.connector;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.sqlrec.common.connector.ChangelogBatchBuffer.Operation.DELETE;
import static com.sqlrec.common.connector.ChangelogBatchBuffer.Operation.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class ChangelogBatchBufferTest {
    @Test
    void keepsWriteDeleteWriteOrderAndHandsOffIndependentBatches() throws Exception {
        List<String> events = new ArrayList<>();
        List<List<String>> writes = new ArrayList<>();
        ChangelogBatchBuffer<String> buffer = new ChangelogBatchBuffer<>(3, 1000,
                batch -> { writes.add(batch); events.add("write:" + batch); },
                batch -> events.add("delete:" + batch));

        buffer.add(WRITE, "old");
        buffer.add(DELETE, "old");
        buffer.add(WRITE, "new");
        buffer.flush();
        buffer.add(WRITE, "later");

        assertEquals(Arrays.asList("write:[old]", "delete:[old]", "write:[new]"), events);
        assertEquals(Arrays.asList("old"), writes.get(0));
        assertEquals(Arrays.asList("new"), writes.get(1));
    }

    @Test
    void intervalIsCheckedByCallerAndBatchFlushDoesNotPostponeIt() throws Exception {
        AtomicLong clock = new AtomicLong();
        List<List<String>> writes = new ArrayList<>();
        ChangelogBatchBuffer<String> buffer = new ChangelogBatchBuffer<>(2, 1000,
                writes::add, batch -> fail("unexpected delete"), clock::get);

        buffer.add(WRITE, "a");
        clock.set(900);
        buffer.add(WRITE, "b");
        assertEquals(1, writes.size());
        buffer.add(WRITE, "c");
        clock.set(1000);
        assertEquals(1, writes.size(), "Elapsed time alone does not trigger a background write");
        buffer.flushIfDue();
        assertEquals(Arrays.asList("c"), writes.get(1));

        buffer.add(WRITE, "d");
        clock.set(1999);
        buffer.flushIfDue();
        assertEquals(2, writes.size());
        clock.set(2000);
        buffer.flushIfDue();
        assertEquals(Arrays.asList("d"), writes.get(2));
    }

    @Test
    void operationSwitchFailurePropagatesBeforeAcceptingTheNextRecord() throws Exception {
        IOException failure = new IOException("write failed");
        List<List<String>> deletes = new ArrayList<>();
        ChangelogBatchBuffer<String> buffer = new ChangelogBatchBuffer<>(2, 1000,
                batch -> { throw failure; }, deletes::add);
        buffer.add(WRITE, "old");

        assertSame(failure, assertThrows(IOException.class, () -> buffer.add(DELETE, "new")));
        buffer.flush();
        assertTrue(deletes.isEmpty(), "The rejected record must be replayed by the caller");
    }
}
