package com.sqlrec.runtime;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteContextReturnStateTest {

    @Test
    void ordinaryClonesShareTheCurrentFunctionReturnState() {
        ExecuteContextImpl outer = new ExecuteContextImpl();
        ExecuteContextImpl function = outer.createFunctionContext();
        ExecuteContextImpl child = function.clone();
        Enumerable<Object[]> result = result(1);

        child.returnFromFunction(result);

        assertTrue(function.hasReturnedFromFunction());
        assertTrue(child.hasReturnedFromFunction());
        assertSame(result, function.getFunctionReturnResult());
        assertFalse(outer.hasReturnedFromFunction());
    }

    @Test
    void differentFunctionFramesAreIsolated() {
        ExecuteContextImpl outer = new ExecuteContextImpl();
        ExecuteContextImpl firstFunction = outer.createFunctionContext();
        ExecuteContextImpl secondFunction = outer.createFunctionContext();

        firstFunction.returnFromFunction(result(1));

        assertTrue(firstFunction.hasReturnedFromFunction());
        assertFalse(secondFunction.hasReturnedFromFunction());
        assertFalse(outer.hasReturnedFromFunction());
    }

    @Test
    void isolatedReturnIsInvisibleUntilCommitted() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        ExecuteContextImpl isolated = function.createIsolatedReturnContext();
        Enumerable<Object[]> result = result(2);

        isolated.returnFromFunction(result);

        assertFalse(function.hasReturnedFromFunction());
        function.commitFunctionReturnFrom(isolated);
        assertTrue(function.hasReturnedFromFunction());
        assertSame(result, function.getFunctionReturnResult());
    }

    @Test
    void committingAnIsolatedContextWithoutReturnIsANoOp() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        function.commitFunctionReturnFrom(function.createIsolatedReturnContext());

        assertFalse(function.hasReturnedFromFunction());
    }

    @Test
    void duplicateReturnIsRejectedWithoutOverwritingFirstResult() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        Enumerable<Object[]> first = result(1);

        function.returnFromFunction(first);
        assertThrows(IllegalStateException.class, () -> function.returnFromFunction(result(2)));

        assertSame(first, function.getFunctionReturnResult());
    }

    @Test
    void committingIntoAnAlreadyReturnedFunctionIsRejected() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        ExecuteContextImpl isolated = function.createIsolatedReturnContext();
        Enumerable<Object[]> first = result(1);
        function.returnFromFunction(first);
        isolated.returnFromFunction(result(2));

        assertThrows(IllegalStateException.class, () -> function.commitFunctionReturnFrom(isolated));
        assertSame(first, function.getFunctionReturnResult());
    }

    @Test
    void returnOutsideAFunctionFrameIsRejected() {
        ExecuteContextImpl context = new ExecuteContextImpl();

        RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> context.returnFromFunction(result(1))
        );

        assertTrue(error.getMessage().contains("inside a SQL function"));
    }

    @Test
    void readingAnUnavailableOrIncompleteReturnStateIsRejected() {
        ExecuteContextImpl outer = new ExecuteContextImpl();
        ExecuteContextImpl function = outer.createFunctionContext();

        assertThrows(IllegalStateException.class, outer::getFunctionReturnResult);
        assertThrows(IllegalStateException.class, function::getFunctionReturnResult);
    }

    @Test
    void commitDefensivelyRejectsInvalidContexts() {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();

        assertThrows(IllegalArgumentException.class, () -> function.commitFunctionReturnFrom(null));
        assertThrows(IllegalArgumentException.class, () -> function.commitFunctionReturnFrom(function.clone()));
        assertThrows(
                IllegalArgumentException.class,
                () -> function.commitFunctionReturnFrom(new ExecuteContextImpl())
        );
    }

    @Test
    void concurrentReturnsAllowExactlyOneWinner() throws Exception {
        ExecuteContextImpl function = new ExecuteContextImpl().createFunctionContext();
        ExecuteContextImpl firstChild = function.clone();
        ExecuteContextImpl secondChild = function.clone();
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        Thread first = returnThread(firstChild, result(1), start, successes, duplicates);
        Thread second = returnThread(secondChild, result(2), start, successes, duplicates);
        first.start();
        second.start();
        start.countDown();
        first.join(2000);
        second.join(2000);

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertEquals(1, successes.get());
        assertEquals(1, duplicates.get());
        assertTrue(function.hasReturnedFromFunction());
    }

    private static Thread returnThread(
            ExecuteContextImpl context,
            Enumerable<Object[]> result,
            CountDownLatch start,
            AtomicInteger successes,
            AtomicInteger duplicates
    ) {
        return new Thread(() -> {
            try {
                start.await();
                context.returnFromFunction(result);
                successes.incrementAndGet();
            } catch (IllegalStateException expected) {
                duplicates.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static Enumerable<Object[]> result(int value) {
        return Linq4j.singletonEnumerable(new Object[]{value});
    }
}
