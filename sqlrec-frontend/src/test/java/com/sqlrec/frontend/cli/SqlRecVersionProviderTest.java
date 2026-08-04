package com.sqlrec.frontend.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlRecVersionProviderTest {

    @Test
    public void testResolveVersionReturnsNonEmpty() {
        String version = SqlRecVersionProvider.resolveVersion();

        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void testGetVersionArrayFormat() {
        String[] versions = new SqlRecVersionProvider().getVersion();

        assertNotNull(versions);
        assertEquals(1, versions.length);
        assertTrue(versions[0].startsWith("sqlrec-cli "));
    }

    @Test
    public void testGetVersionContainsResolvedVersion() {
        String resolved = SqlRecVersionProvider.resolveVersion();
        String[] versions = new SqlRecVersionProvider().getVersion();

        assertTrue(versions[0].contains(resolved));
    }
}
