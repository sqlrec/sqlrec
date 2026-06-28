package com.sqlrec.frontend.cli;

import com.sqlrec.common.config.SqlRecConfigs;
import picocli.CommandLine.IVersionProvider;

/**
 * Picocli version provider that reads from the shared {@link SqlRecConfigs#SQLREC_VERSION}
 * config, so {@code -V/--version} and the REPL banner share a single source of truth.
 * <p>
 * Resolution order: env var {@code SQLREC_VERSION} → config default value.
 */
public class SqlRecVersionProvider implements IVersionProvider {

    /**
     * Resolve the CLI version string from {@link SqlRecConfigs#SQLREC_VERSION}.
     * Falls back to the config's default value if the env-var lookup fails.
     */
    public static String resolveVersion() {
        try {
            return SqlRecConfigs.SQLREC_VERSION.getValue();
        } catch (Exception e) {
            return SqlRecConfigs.SQLREC_VERSION.getDefaultValue();
        }
    }

    @Override
    public String[] getVersion() {
        return new String[]{"sqlrec-cli " + resolveVersion()};
    }
}
