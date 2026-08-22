package com.sqlrec.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecuteContextImplCancelTest {

    @Test
    public void testCancelMarksSelfAndAllDescendants() {
        ExecuteContextImpl root = new ExecuteContextImpl();
        ExecuteContextImpl child = root.clone();
        ExecuteContextImpl grandChild = child.clone();

        assertFalse(root.isCancelled());
        assertFalse(child.isCancelled());
        assertFalse(grandChild.isCancelled());

        root.cancel();

        assertTrue(root.isCancelled());
        assertTrue(child.isCancelled());
        assertTrue(grandChild.isCancelled());
    }

    @Test
    public void testCancelChildDoesNotAffectAncestorOrSibling() {
        ExecuteContextImpl root = new ExecuteContextImpl();
        ExecuteContextImpl childA = root.clone();
        ExecuteContextImpl childB = root.clone();
        ExecuteContextImpl grandChildOfA = childA.clone();

        childA.cancel();

        assertTrue(childA.isCancelled());
        assertTrue(grandChildOfA.isCancelled());
        assertFalse(root.isCancelled());
        assertFalse(childB.isCancelled());
    }

    @Test
    public void testCloneAfterCancelSeesCancelled() {
        // even if cancel races with clone, the newly cloned child context still sees the cancellation via the parent chain
        ExecuteContextImpl root = new ExecuteContextImpl();
        root.cancel();
        ExecuteContextImpl child = root.clone();
        assertTrue(child.isCancelled());
    }

    @Test
    public void testCancelIsIdempotent() {
        ExecuteContextImpl root = new ExecuteContextImpl();
        root.cancel();
        root.cancel();
        assertTrue(root.isCancelled());
    }

    @Test
    public void testDeepChainCancelled() {
        ExecuteContextImpl root = new ExecuteContextImpl();
        ExecuteContextImpl c1 = root.clone();
        ExecuteContextImpl c2 = c1.clone();
        ExecuteContextImpl c3 = c2.clone();
        ExecuteContextImpl c4 = c3.clone();
        ExecuteContextImpl siblingOfC2 = c1.clone();

        root.cancel();
        assertTrue(c4.isCancelled());

        root = new ExecuteContextImpl();
        c1 = root.clone();
        c2 = c1.clone();
        siblingOfC2 = c1.clone();
        c1.cancel();
        assertTrue(c2.isCancelled());
        assertTrue(siblingOfC2.isCancelled());
    }

    @Test
    public void testCloneStillSharesVariablesAndFunNameStack() {
        // regression: clone semantics unchanged (shares variables/metricsTags, copies funNameStack)
        ExecuteContextImpl root = new ExecuteContextImpl();
        root.setVariable("k", "v");
        root.addFunNameToStack("fun1");

        ExecuteContextImpl child = root.clone();
        child.setVariable("k2", "v2");
        child.addFunNameToStack("fun2");

        assertEquals("v", root.getVariable("k"));
        assertEquals("v2", child.getVariable("k2"));
        assertEquals("v2", root.getVariable("k2"));
        // stack changes on the child do not affect the parent context
        assertEquals(1, root.getFunNameStack().size());
        assertEquals(2, child.getFunNameStack().size());
    }
}
